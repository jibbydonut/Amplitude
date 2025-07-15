# Data extraction using Aplitude's Export API
# https://amplitude.com/docs/apis/analytics/export 

import requests
import os
import datetime
from dotenv import load_dotenv
import logging
from modules.helper import setup_logger
import time
import zipfile
import gzip
import shutil
import tempfile

setup_logger()

api_key = os.getenv("AMP_API_KEY")
secret_key = os.getenv("AMP_SECRET_KEY")

end_time = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).strftime('%Y%m%dT23')
start_time = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).strftime('%Y%m%dT00')

def export_api(start_time, end_time, api_key, secret_key, output_file='data.zip'):
    # Load .env and assign variables
    load_dotenv()

    amp_endpoint = "https://analytics.eu.amplitude.com/api/2/export"

    params = {
        'start': start_time, # Format YYYYMMDDTTT
        'end': end_time
    }

    # Set save filepath
    output_dir = os.path.join(os.getcwd(),'local\/raw_zip')
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, end_time + '_' +output_file)

    for i in range(3):
        try:
            response = requests.get(amp_endpoint,params=params,auth=(api_key,secret_key))
            response.raise_for_status()
            
            data = response.content

            try: #catches File write failures
                with open(filepath,'wb') as file:
                    file.write(data)
            except IOError as file_err:
                logging.error(f"File write failed: {file_err}")
                return False

            logging.info(f"Data saved to {filepath}")
            return True
        except requests.exceptions.RequestException as e:
            logging.error(f"Attempt {i} : Failed to get data - {e}")
            time.sleep(2**i)
    return False

def extract_json_from_zip():
    # Extract JSON from ZIP

    # Create temp directory for extraction
    temp_dir = tempfile.mkdtemp()
    logging.info('Temp_dir '+temp_dir+" created")

    json_dir = os.path.join(os.getcwd(),'local/json')
    os.makedirs(json_dir, exist_ok=True)

    zip_path = 'local/raw_zip/'+(datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).strftime('%Y%m%dT23')+'_data.zip'

    with zipfile.ZipFile(zip_path,"r") as zip_ref:
        zip_ref.extractall(temp_dir)

    # locate folder -> need file path
    day_folder = next(f for f in os.listdir(temp_dir) if f.isdigit()) # alt: os.listdir(temp_dir)[0] - takes the first folder
    day_path = os.path.join(temp_dir, day_folder)

    #unpack items
    json_count = 0
    for root, _, files in os.walk(day_path):
        for file in files:
            if file.endswith(".gz"):
                gz_path = os.path.join(root,file)
                json_filename = file[:-3]
                output_path = os.path.join(json_dir,json_filename)
                with gzip.open(gz_path,'rb') as gz_file, open(output_path,'wb') as out_file:
                    shutil.copyfileobj(gz_file,out_file)
                json_count +=1
    
    logging.info(f"{json_count} JSON files extracted from {zip_path} to {json_dir}")

    # Delete .zip after run
    os.remove(zip_path)
    logging.info(f"Deleted ZIP file: {zip_path}_data.zip")


# if export_api():
#     extract_json_from_zip()
# else:
#     logging.warning('Skipping JSON extraction because export_api() failed.')




