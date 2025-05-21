import argparse
import os
import dotenv
from data_import import data_import, get_importer_impl

CONFIG_FOLDER = 'config/'
dotenv.load_dotenv()

def get_config_files(config_folder: str) -> list:
    return [os.path.join(config_folder, f) for f in os.listdir(config_folder) if f.endswith(('.json', '.yaml', '.yml'))]


def process_single_config(config_file: str):
    try:
        print(f"Processing configuration: {config_file}")
        data_import(config_file)
    except Exception as e:
        print(f"Failed to process configuration {config_file}: {e}")


def process_all_configs():
    config_files = get_config_files(CONFIG_FOLDER)

    if not config_files:
        print("No configuration files found.")
        return

    for config_file in config_files:
        process_single_config(config_file)


def verify_collection(config_file: str):
    try:
        print(f"Verifying collection from config: {config_file}")
        importer = get_importer_impl(config_file)
        importer.verify_collection()
    except Exception as e:
        print(f"Failed to verify collection {config_file}: {e}")


def verify_data(config_file: str):
    try:
        print(f"Verifying data from config: {config_file}")
        importer = get_importer_impl(config_file)
        importer.verify_data()
    except Exception as e:
        print(f"Failed to verify data {config_file}: {e}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="CLI tool for importing data into OpenWebUI knowledge base using specified configurations."
    )

    parser.add_argument(
        '--config',
        type=str,
        help="Path to a specific configuration file. If not provided, all configurations in the config folder will be processed."
    )

    parser.add_argument(
        '--list',
        action='store_true',
        help="List all available configuration files in the config directory."
    )

    parser.add_argument(
        '--verify',
        type=str,
        help="Verify the connection to the knowledge base and list all data with statistics."
    )

    return parser.parse_args()


def exec_verification(config_file: str):
    verify_collection(config_file)
    verify_data(config_file)
    print("Verification complete.")


def main():
    args = parse_args()

    if args.list:
        config_files = get_config_files(CONFIG_FOLDER)
        print("Available configuration files:")
        for config_file in config_files:
            print(f"- {os.path.basename(config_file)}")
        return

    if args.config:
        if os.path.isfile(args.config):
            process_single_config(args.config)
        else:
            print(f"Configuration file {args.config} not found.")
    elif args.verify:
        if os.path.isfile(args.verify):
            print(f"Verifying configuration file {args.verify}")
            exec_verification(args.verify)
        else:
            print(f"Configuration file {args.verify} not found.")
    else:
        process_all_configs()


if __name__ == "__main__":
    main()