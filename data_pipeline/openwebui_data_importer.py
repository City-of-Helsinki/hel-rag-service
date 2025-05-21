import os
import tempfile
import time
from abc import ABC, abstractmethod
import requests
import logging
import tiktoken

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class OpenWebUIDataImporter(ABC):


    def __init__(self, config: dict):
        self.config = config
        # Use token from env variable if available
        try:
            if 'OPEN_WEB_UI_API_KEY' in os.environ:
                logging.info("Using API key from environment variable.")
                self.token = os.environ['OPEN_WEB_UI_API_KEY']
            else:
                logging.info("No token found in environment variable. Try to use token from config.")
                self.token = config['openwebui']['token']
        except:
            logging.error("No token found in config or environment variable.")
            self.token = None

        # Use base url from env variable if available
        try:
            if 'OPEN_WEB_UI_BASE_URL' in os.environ:
                logging.info("Using Base URL from environment variable.")
                self.base_url = os.environ['OPEN_WEB_UI_BASE_URL']
            else:
                logging.info("No base url found in environment variable. Try to use base url from config.")
                self.base_url = config['openwebui']['base_url']
        except:
            logging.error("No token found in config or environment variable.")
            self.base_url = None

        self.knowledge_name = config['openwebui']['knowledge_name']
        self.file_upload_url = f"{self.base_url}/api/v1/files/"
        self.knowledge_url = f"{self.base_url}/api/v1/knowledge"
        self.token_counts = []
        self.tokenizer = tiktoken.get_encoding("o200k_base")
        logging.debug("Initialized BaseDataImporter with config:")
        logging.debug(f"Base url: {self.base_url}")
        logging.debug(f"Knowledge name: {self.knowledge_name}")

    @abstractmethod
    def read_data(self):
        pass

    def record_token_count(self, text):
        tokens = self.tokenizer.encode(text)
        self.token_counts.append(len(tokens))

    def calculate_average_token_count(self):
        if not self.token_counts:
            return 0
        return sum(self.token_counts) / len(self.token_counts)

    def get_headers(self, content_type='application/json'):
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': content_type
        }
        return headers

    import time

    def upload_file(self, file_content, file_name):
        logging.info(f"Uploading file: {file_name}")
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Accept': 'application/json'
        }
        self.record_token_count(file_content)

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(file_content.encode('utf-8'))
            temp_file_path = temp_file.name
            logging.info(f"Temporary file created at: {temp_file_path}")

        retries = 3
        for attempt in range(retries):
            try:
                with open(temp_file_path, 'rb') as temp_file:
                    files = {'file': (file_name, temp_file)}
                    response = requests.post(self.file_upload_url, headers=headers, files=files)
                    logging.info(f"File uploaded: {file_name}")
                if response.status_code == 405:
                    logging.error(f"HTTP 405 Method Not Allowed: {response.text}")
                else:
                    response.raise_for_status()
                    logging.info(f"File uploaded successfully: {file_name}")
                    return response.json()
            except requests.exceptions.RequestException as e:

                # If error is related to duplicate content, skip it
                error_text = "Duplicate content detected."
                if error_text in str(e):
                    logging.warning(f"Duplicate content detected for file: {file_name}. Skipping upload.")
                    # do not attempt to upload again
                    break

                logging.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logging.error(f"All {retries} attempts failed")
                    raise
            finally:
                os.remove(temp_file_path)
                logging.info(f"Temporary file removed: {temp_file_path}")

    def add_file_to_knowledge(self, knowledge_id, file_id):
        url = f"{self.knowledge_url}/{knowledge_id}/file/add"
        headers = self.get_headers()
        data = {'file_id': file_id, 'source_id': file_id}
        logging.info(f"Adding file to knowledge: {knowledge_id}, file_id: {file_id}")
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        logging.info(f"File added to knowledge: {knowledge_id}, file_id: {file_id}")
        return response.json()

    def update_knowledge_with_data(self, data):
        logging.info("Updating knowledge with data")
        knowledge = self.get_knowledge_by_name()
        if knowledge:
            logging.info(f"Knowledge found: {knowledge['id']}, resetting knowledge")
            self.reset_knowledge(knowledge['id'])
        else:
            logging.info("Knowledge not found, creating new knowledge")
            knowledge = self.create_knowledge()

        for entry in data:
            try:
                file_content = entry['content']
                file_name = entry['file_name']
                try:
                    # Attempt to pick prefix from the pipeline_config
                    file_name = self.config['data']['prefix'] + file_name
                except:
                    pass
                upload_response = self.upload_file(file_content, file_name)
                file_id = upload_response['id']
                self.add_file_to_knowledge(knowledge['id'], file_id)
            except Exception as e:
                logging.error(f"Failed to upload file: {e}")
        logging.info("Knowledge updated with data")
        logging.info("Mean content size in tokens: " + str(self.calculate_average_token_count()))

    def get_knowledge_by_name(self):
        headers = self.get_headers()
        logging.info(f"Getting knowledge by name: {self.knowledge_name}")
        response = requests.get(f"{self.knowledge_url}/list", headers=headers)
        response.raise_for_status()
        knowledge_list = response.json()
        for knowledge in knowledge_list:
            if knowledge['name'] == self.knowledge_name:
                logging.info(f"Knowledge found: {knowledge}")
                return knowledge
        logging.info("Knowledge not found")
        return None

    def create_knowledge(self):
        headers = self.get_headers()
        knowledge_data = {
            "name": self.knowledge_name,
            "description": f"Knowledge base for {self.knowledge_name}"
        }
        logging.info(f"Creating knowledge: {knowledge_data}")
        response = requests.post(f"{self.knowledge_url}/create", headers=headers, json=knowledge_data)
        response.raise_for_status()
        logging.info(f"Knowledge created: {response.json()}")
        return response.json()

    def reset_knowledge(self, knowledge_id):
        headers = self.get_headers()
        logging.info(f"Resetting knowledge: {knowledge_id}")
        response = requests.post(f"{self.knowledge_url}/{knowledge_id}/reset", headers=headers)
        response.raise_for_status()
        logging.info(f"Knowledge reset: {knowledge_id}")
        # wait for 10 seconds to ensure the reset is complete
        time.sleep(10)

    def verify_collection(self):
        try:
            logging.debug(f"Verifying collection for knowledge base: {self.knowledge_name}")
            headers = self.get_headers()
            response = requests.get(f"{self.base_url}/api/v1/knowledge/list", headers=headers)
            response.raise_for_status()
            knowledge_list = response.json()

            logging.debug(f"Connected to knowledge base. Found {len(knowledge_list)} knowledge entries.")
            if knowledge_list and len(knowledge_list) > 1:
                first_entry = knowledge_list[0]
                last_entry = knowledge_list[-1]
                logging.debug(f"First entry: id {first_entry['id']}")
                logging.debug(f"Last entry: id {last_entry['id']}")
            else:
                logging.info("No entries found in the knowledge base.")
            logging.info(f"Verification for knowledge base: {self.knowledge_name}: PASS")
        except Exception as e:
            logging.error(f"Failed to verify collection: {e}")
            logging.debug("Knowledge not found")
            logging.info(f"Verification for knowledge base: {self.knowledge_name}: FAIL")

    def verify_data(self):
        try:
            logging.debug(f"Verifying data for knowledge base: {self.knowledge_name}")
            data = self.read_data()
            if not data:
                raise ValueError("No data found.")

            logging.debug(f"Found {len(data)} entries.")
            logging.info(f"Verification for data PASS")
        except Exception as e:
            logging.error(f"Failed to verify data: {e}")
            logging.info(f"Verification for data FAIL")

    def safe_filename(self, file_name):
        return file_name.replace(' ', '_').replace('/', '_').replace('\\', '_').replace(':', '_')