import schedule
import time
import subprocess
import os
import logging
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_import(config_path):
    try:
        logging.info(f"Starting import with config: {config_path}")
        subprocess.run(["python", "data_pipeline.py", "--config", config_path], check=True)
        logging.info(f"Completed import with config: {config_path}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error during import with config: {config_path} - {e}")

def main(config_dir):
    # Read the cron schedule from the environment variable or use the default
    import_time = os.getenv('DATA_IMPORT_TIME', '12:23')
    logging.info("----- Import Scheduler -----")
    logging.info(f"Begin data imports daily at: {import_time}")
    logging.info(f"Configuration directory: {config_dir}")
    logging.info("Current time: %s", time.strftime("%Y-%m-%d %H:%M:%S"))
    logging.info("----------------------------")

    # List all configuration files in the directory
    config_files = [os.path.join(config_dir, f) for f in os.listdir(config_dir) if f.endswith('.json')]

    # Schedule the task for each configuration file
    for config_path in config_files:
        schedule.every().day.at(import_time).do(run_import, config_path=config_path)

    # Run the scheduler
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run import scheduler for all configuration files in a specified directory.")
    parser.add_argument('config_dir', type=str, help="Path to the configuration directory.")
    args = parser.parse_args()

    main(args.config_dir)