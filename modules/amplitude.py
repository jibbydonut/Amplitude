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
import boto3

load_dotenv()

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

def extract_json_from_zip(start_time,end_time,output_file='data.zip'):
    # Extract JSON from ZIP, return boolean on pass/fail

    try:
        # Create temp directory for extraction
        temp_dir = tempfile.mkdtemp()
        logging.info('Temp_dir '+temp_dir+" created")

        json_dir = os.path.join(os.getcwd(),'local/json')
        os.makedirs(json_dir, exist_ok=True)

        zip_path = 'local/raw_zip/'+end_time+'_'+output_file

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
        return True

    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        return False

def upload_json_to_s3(file_path, bucket, sub_dir,aws_access, aws_secret_access):

    s3_client = boto3.client(
        service_name='s3',
        aws_access_key_id=aws_access,
        aws_secret_access_key=aws_secret_access
    )

    uploaded_cnt = 0

    for root, _, files in os.walk(file_path):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                object_name = f"{sub_dir}/{file}"


                try:
                    s3_client.upload_file(file_path, bucket, object_name)
                    logging.info(f"File uploaded to S3: s3://{bucket}/{sub_dir}/{object_name}")
                    uploaded_cnt += 1
                except boto3.exceptions.S3UploadFailedError as e:
                    logging.error(f"S3 upload failed: {e}")
                except Exception as e:
                    logging.error(f"Unexpected error during S3 upload: {e}")
    
    logging.info(f"Total Json Uploaded: {uploaded_cnt}")

api_key = os.getenv("AMP_API_KEY")
secret_key = os.getenv("AMP_SECRET_KEY")

file_path = "local/json"

aws_access = os.getenv('ACCESS_KEY')
aws_secret = os.getenv('SECRET_ACCESS_KEY')
bucket_name = os.getenv("AWS_BUCKET_NAME")
print(bucket_name)
sub_dir = 'python_import'


# List all files in S3
def list_s3_objects(bucket_name, prefix):
    # Set s3 client
    s3_client = boto3.client(
        service_name='s3',
        aws_access_key_id=aws_access,
        aws_secret_access_key=aws_secret
    )

    paginator = s3_client.get_paginator('list_objects_v2')
    filenames = []

    # Iterate through files
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get('Contents', []):
            full_key = obj['Key']
            filename = os.path.basename(full_key)  # Gets just the filename
            filenames.append(filename)
    return filenames


local_dir = "local\json"

def list_local_files(local_dir):

    # List local json files
    file_paths = []

    for root, _, files in os.walk(local_dir):
        for name in files:
            full_path = os.path.join(root, name)
            rel_path = os.path.relpath(full_path, local_dir).replace("\\","/")
            file_paths.append(rel_path)

    return file_paths

# local_list = list_local_files(local_dir=local_dir)
# # print(local_list)
# s3_list = list_s3_objects(bucket_name=bucket_name,prefix=sub_dir)
# print(s3_list)

missing_files = [f for f in list_local_files(local_dir=local_dir) if f not in list_s3_objects(bucket_name=bucket_name,prefix=sub_dir)]

print(missing_files)


