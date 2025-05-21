import json
import os
from csv_excel_importer import CSVExcelImporter
from events_api_importer import EventsAPIImporter

from web_scraper_importer import WebScraperImporter


def get_importer_impl(config_path):
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    config = load_config(config_path)
    importer_type = config['type']

    if importer_type == 'csv_excel':
        importer = CSVExcelImporter(config)
    elif importer_type == 'web':
        importer = WebScraperImporter(config)
    elif importer_type == 'events_api':
        importer = EventsAPIImporter(config)
    else:
        raise ValueError(f"Unsupported configuration type: {importer_type}")
    return importer


def data_import(config_path: str):
    importer_impl = get_importer_impl(config_path)
    data = importer_impl.read_data()
    importer_impl.update_knowledge_with_data(data)

def load_config(config_file: str) -> dict:
    extension = os.path.splitext(config_file)[1].lower()
    with open(config_file, 'r', encoding='utf-8') as file:
        if extension == '.json':
            return json.load(file)
        else:
            raise ValueError("Unsupported config file format. Please use JSON or YAML.")