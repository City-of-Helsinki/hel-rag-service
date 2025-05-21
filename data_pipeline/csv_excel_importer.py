# csv_excel_importer.py
import datetime
import os
import pandas as pd

from openwebui_data_importer import OpenWebUIDataImporter


class CSVExcelImporter(OpenWebUIDataImporter):

    def read_data(self):
        file_path = self.config['data']['file_path']
        content_fields = self.config['data']['content_fields']
        entry_config = self.config['data']['entry_config']

        filter_field = None
        filter_value = None
        filter_condition = self.config['data']['filter_condition']
        if filter_condition:
            if 'filter_field' not in filter_condition or 'filter_value' not in filter_condition:
                raise ValueError("Filter condition must contain 'filter_field' and 'filter_value'.")
            filter_field = filter_condition['filter_field']
            filter_value = filter_condition['filter_value']

        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.csv':
            df = pd.read_csv(file_path)
        elif file_extension in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path)
        else:
            raise ValueError("Unsupported file type. Use CSV or Excel files.")

        data_json = []
        for _, row in df.iterrows():
            # Skip rows that do not match the filter condition
            if filter_field and filter_value:
                if row[filter_field] != filter_value:
                    continue
            title = row[entry_config['title_field']] if entry_config['title_field'] in row else "Untitled"
            text_content = f"{title}\n\n" + ' '.join(f"\n\n{field}: {row[field]}" for field in content_fields if field in row)
            text_content = text_content.replace("nan", "-")
            file_config = {'date_imported': datetime.datetime.now().isoformat()}
            for key, new_key in entry_config.items():
                if key in row:
                    file_config[new_key] = row[key]
            file_name = row[entry_config['file_name_field']] if entry_config['file_name_field'] in row else f"unknown_entry_{_ + 1}"
            file_name = self.safe_filename(file_name)
            data_json.append({"content": text_content, "file_config": file_config, "file_name": file_name})

        return data_json

    def safe_filename(self, file_name):
        return file_name.replace(' ', '_').replace('/', '_').replace('\\', '_').replace(':', '_')