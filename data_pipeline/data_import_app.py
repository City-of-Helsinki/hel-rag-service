from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
import subprocess
import os
import logging
from datetime import datetime

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Dictionary to store the last update dates
last_update_dates = {}

class ImportRequest(BaseModel):
    config_path: str

def run_import(config_path):
    try:
        logging.info(f"Starting import with config: {config_path}")
        subprocess.run(["python", "data_pipeline.py", "--config", config_path], check=True)
        logging.info(f"Completed import with config: {config_path}")
        last_update_dates[config_path] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error during import with config: {config_path} - {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    config_dir = os.getenv("SCHEDULED_IMPORTS_PATH", "/app/config/scheduled_imports/")  # Update this path as needed
    scheduler = BackgroundScheduler()
    import_time = os.getenv('DATA_IMPORT_TIME', '18:00')
    config_files = [os.path.join(config_dir, f) for f in os.listdir(config_dir) if f.endswith('.json')]

    for config_path in config_files:
        scheduler.add_job(run_import, 'cron', hour=import_time.split(':')[0], minute=import_time.split(':')[1], args=[config_path])

    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

@app.get("/status")
def get_status():
    return last_update_dates

@app.post("/trigger-import")
def trigger_import(request: ImportRequest):
    config_path = os.getenv("ONE_TIME_IMPORTS_PATH", "/app/config/one_time_imports/")
    if not os.path.isfile(config_path):
        raise HTTPException(status_code=400, detail="Configuration file not found")
    run_import(config_path)
    return {"message": "Import triggered", "config_path": config_path}

@app.get("/")
def ok():
    return "OK"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8001)