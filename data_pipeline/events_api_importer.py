import datetime
import uuid
import requests
import logging
from bs4 import BeautifulSoup
from openwebui_data_importer import OpenWebUIDataImporter

class EventsAPIImporter(OpenWebUIDataImporter):
    '''
    Fetch data from Events API and web pages related to the events in Helsinki
    API URL: URL for events listing. Defined in the config file.
    Base URL: defined in the config file
    '''

    def read_data(self):
        api_url = self.config['data']['api_url']
        logging.info(f"Fetching data from URL: {api_url}")
        try:
            response = requests.get(api_url)
            response.raise_for_status()
            events_data = response.json()
            events = events_data.get('data', [])
            data_json = []

            for event in events:
                event_url = event.get('location', {}).get('@id', None)
                title = event.get('name', {}).get('fi', 'No Title')
                start_time = datetime.datetime.fromisoformat(event.get('start_time', '1970-01-01T00:00:00'))
                end_time = datetime.datetime.fromisoformat(event.get('end_time', '1970-01-01T00:00:00'))
                location = event.get('location', {}).get('name', {}).get('fi', 'No Location')
                street_address = event.get('location', {}).get('street_address', {}).get('fi', 'No Address')
                description = event.get('description', {}).get('fi', None)

                if not description and event_url:
                    response = requests.get(event_url)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, 'html.parser')
                    article_content = soup.find('main').get_text(separator=" ") if soup.find('main') else None
                    description = f"{article_content}"

                text_content = f"""
                    Tapahtuma: {title}
                    , \n
                    Päivämäärä: {start_time.strftime('%d.%m.%Y')}
                    , \n
                    Aika: {start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}
                    , \n
                    Paikka: {location}, {street_address}
                    , \n
                    Kuvaus: {description}
                    """

                file_name = self.safe_filename(f"{start_time.strftime('%Y-%m-%d')}_{title[:6]}") + "_" + uuid.uuid4().hex[:6]
                file_config = {'date_imported': datetime.datetime.now().isoformat()}

                data_json.append({"content": text_content, "file_config": file_config, "file_name": file_name})
                logging.info(f"Processed event: {title}")

            return data_json

        except Exception as e:
            logging.error(f"Failed to fetch or process data from URL: {api_url}, error: {e}")
            return []