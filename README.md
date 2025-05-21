# hel-rag-service
Modular AI assistant environment for RAG (Retrieval-Augmented Generation) chat, built on top of OpenWebUI and LiteLLM. 
It provides a user-friendly interface for interacting with language models enhanced by external data sources, 
making it suitable for building contextual, domain-aware chat applications. Has tools to import data from CSV/Excel files 
and web scraping to OpenWebUI knowledge base.

## Configuration

1. Copy `.env.example` to `.env` and fill in the values.
2. Copy `litellm_config.yaml_template` to `litellm_config.yaml` and configure the models and api endpoints. 
   - Example configuration for Azure OpenAI:
   ```yaml
   model_list:
     - model_name: azure-gpt-4o
       litellm_params:
         model: azure/gpt-4o # Format: azure/{model_name}
         api_base: https://XYZ.openai.azure.com/ # Your Azure API endpoint. Note this cannot be environment variable.
         api_key: API_KEY # Your Azure API key. This can be environment variable or hardcoded. Values will end up to model configurations in database.
         api_version: 2024-02-15-preview # Your Azure API version
   ```
3. Run `docker compose up` to start the application.
4. Open the browser and navigate to http://localhost:3001 to access the chat application.
5. Create admin user for the chat application by registering a new user. The first user will become the admin user.
6. Verify that chat is working and you can find all the models configured in litellm_config.yaml.

    
## Data Pipeline

The data pipeline is used to import data into the OpenWebUI knowledge base. It supports different data sources such as CSV/Excel files and web scraping. Below are the steps to configure and run the data pipeline.

### Configuration

1. **Create Configuration File**: Create a configuration file in the `config` folder. The configuration file should be in JSON or YAML format and should define the data source type, OpenWebUI settings, and data-specific settings.

   Example configuration for a CSV/Excel data source (`config/dev/config_templates/excel-config.json`):
   ```json
   {
     "type": "csv_excel",
     "openwebui": {
       "knowledge_name": "excel_data_knowledge"
     },
     "data": {
       "file_path": "rag_source_data/input_file.xlsx",
       "content_fields": [
         "Column name 1",
         "Column name 2"
       ],
       "entry_config": {
         "file_name_field": "Column name 1",
         "title_field": "Column name 1"
       },
       "field_mapping": {}
     }
   }
   ```

2. **Web Scraping Configuration**: If you are using web scraping, create a configuration file similar to the following example (`config/web_scraper_config.json`):
   ```json
   {
     "type": "web",
     "openwebui": {
       "base_url": "http://localhost:3001",
       "token": "your_token_here",
       "knowledge_name": "web_scraper_knowledge"
     },
     "data": {
       "base_url": "https://example.com",
       "paths": ["/page1", "/page2", "/page3"],
       "content_selectors": [".content", ".description"],
       "entry_config": {
         "title_selector": "h1.title",
         "file_name_selector": "h1.title"
       }
     }
   }
   ```

### Setup notes

- Ensure that the environment variables `OPEN_WEB_UI_BASE_URL` and `OPEN_WEB_UI_API_KEY` are correctly set to connect to your OpenWebUI instance.
- The `knowledge_name` should be unique for each knowledge base you create.
- The `data` section in the configuration file should be tailored to the specific data source you are importing.


### Setting up API keys

You need a API key in order to communicate with OpenWebUI api. You can generate one in user interface.

0. Go to admin panel, eg.  http://localhost:3001/admin/settings 
1. Enable API access for the admin user in the admin panel. "Enable API key"
2. Set restrictions for the API key in the admin panel. "Allowed endpoints"
3. Get the api key from Admin -> Settings -> Account -> Api key


### Running the Data Pipeline

1. **Install Dependencies**: Ensure you have the required dependencies installed. You can install them using pip:
   ```sh
   pip install -r requirements.txt
   ```

2. **Run the Data Pipeline**: Use the following command to run the data pipeline with a specific configuration file:
   ```sh
   python data_pipeline.py --config config/dev/config_templates/excel-config.json
   ```

   To process all configuration files in the `config` folder, run:
   ```sh
   python data_pipeline.py
   ```

3. **Verify Collection**: To verify the connection to the knowledge base and list all data with statistics, use the `--verify` option:
   ```sh
   python data_pipeline.py --verify cconfig/dev/config_templates/excel-config.json
   ```

4. **List Available Configurations**: To list all available configuration files in the `config` directory, use the `--list` option:
   ```sh
   python data_pipeline.py --list
   ```



## OpenWebUI RAG configuration 

1. Create knowledge or use data pipeline to import data
2. Create custom model in openwebui workspace
3. Attach knowledge(s) to custom model configuration
4. Adjust the RAG configuration in OpenWebUI to fit your needs.
5. Set RAG settings: K = 10 (amount of documents to retrieve per knowledge)
6. Set RAG settings: Chunk size = 400 tokens (Run data_import to see what is the average content length in tokens)
7. Set RAG settings: Chunk overlap = 50 tokens

##  OpenWebUI Functions & filters

1. Import functions using exported functions (functions-export-xyz.json) from the `functions` dir. 
2. Enable function
3. Set custom model to use function:
4. Workspace -> Models -> Edit model
5. Enable function


## OpenWebUI Environment variables

Here are some recommended environment variables for configuring the OpenWebUI application. These variables can be set in the `.env` file or directly in your Docker Compose file or to Azure Container Service settings.

See https://docs.openwebui.com/getting-started/env-configuration/

```bash  
OPENAI_API_BASE_URL=<litellm container url>
OPENAI_API_KEY=<litellm api key>
RAG_OPENAI_API_BASE_URL=<litellm container url>
RAG_OPENAI_API_KEY=<litellm api key>
ENABLE_ADMIN_EXPORT=false
ENABLE_ADMIN_CHAT_ACCESS=false
DEFAULT_USER_ROLE=pending
DEFAULT_LOCALE=fi
ENABLE_OLLAMA_API=false
TASK_MODEL=azure-gpt-4o-mini
TITLE_GENERATION_PROMPT_TEMPLATE="Summarize in few words the topic of this conversation using the language of conversation."
ENABLE_AUTOCOMPLETE_GENERATION=false
ENABLE_EVALUATION_ARENA_MODELS=false
ENABLE_COMMUNITY_SHARING=false
ENABLE_TAGS_GENERATION=false
ENABLE_FORWARD_USER_INFO_HEADERS=false
RAG_TOP_K=10
RAG_RELEVANCE_THRESHOLD=0.51
RAG_TEXT_SPLITTER=token
CHUNK_SIZE=400
CHUNK_OVERLAP=50
RAG_EMBEDDING_OPENAI_BATCH_SIZE=1
ENABLE_API_KEY_ENDPOINT_RESTRICTIONS=false
ANONYMIZED_TELEMETRY=false
```      

## Pipelines extension

The Pipelines extension in OpenWebUI allows you to define and run custom Python-based workflows for advanced automation and integration scenarios. Pipelines can be used to implement complex logic, interact with external APIs, or perform multi-step tasks that go beyond standard chat or RAG functionality.

Location: Place your pipeline implementations in the custom_pipelines directory. For example, see custom_pipelines/example_pipeline/example_pipeline.py for a template.
Usage: Copy or create your own pipeline Python files under custom_pipelines. The OpenWebUI container will automatically detect and load them.
Documentation: For more details on how to create and use pipelines, refer to the official documentation: https://docs.openwebui.com/pipelines/.

