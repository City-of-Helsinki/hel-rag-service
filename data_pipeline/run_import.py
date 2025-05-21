import logging
import os
import subprocess
import argparse

from dotenv import load_dotenv

from data_import import data_import

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()
def execute_pipeline(config_file):
    try:
        result = subprocess.run(['python', 'data_pipeline.py', '--config', config_file], check=True, capture_output=True, text=True)
        print(f"Execution of {config_file} succeeded:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Execution of {config_file} failed:\n{e.stderr}")

def main(config_dir):
    # List all configuration files in the directory
    config_files = [os.path.join(config_dir, f) for f in os.listdir(config_dir) if f.endswith('.json')]

    # Run the pipeline for each configuration file
    for config_file in config_files:
        data_import(config_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run import pipeline for all configuration files in a specified directory.")
    parser.add_argument('config_dir', type=str, help="Path to the configuration directory.")
    args = parser.parse_args()
    print("Running import pipeline for all configuration files in the specified directory: ", args.config_dir)
    main(args.config_dir)