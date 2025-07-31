# Data extraction using Amplitude's Export API
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
from contextlib import contextmanager

load_dotenv()
setup_logger()

api_key = os.getenv("AMP_API_KEY")
secret_key = os.getenv("AMP_SECRET_KEY")

end_time = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).strftime('%Y%m%dT23')
start_time = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).strftime('%Y%m%dT00')

@contextmanager
def temp_directory():
    """Context manager for creating and cleaning up temporary directories"""
    temp_dir = tempfile.mkdtemp()
    try:
        logging.info(f'Temporary directory created: {temp_dir}')
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
        logging.info(f'Temporary directory cleaned up: {temp_dir}')

def export_api(start_time, end_time, api_key, secret_key, temp_dir, output_file='data.zip'):
    """Export data from Amplitude API to a temporary directory"""
    
    amp_endpoint = "https://analytics.eu.amplitude.com/api/2/export"
    
    params = {
        'start': start_time,  # Format YYYYMMDDTTT
        'end': end_time
    }
    
    # Create zip subfolder in temp directory
    zip_dir = os.path.join(temp_dir, 'raw_zip')
    os.makedirs(zip_dir, exist_ok=True)
    filepath = os.path.join(zip_dir, f"{end_time}_{output_file}")
    
    for i in range(3):
        try:
            response = requests.get(amp_endpoint, params=params, auth=(api_key, secret_key))
            response.raise_for_status()
            
            data = response.content
            
            try:  # catches File write failures
                with open(filepath, 'wb') as file:
                    file.write(data)
            except IOError as file_err:
                logging.error(f"File write failed: {file_err}")
                return False, None
            
            logging.info(f"Data saved to {filepath}")
            return True, filepath
        except requests.exceptions.RequestException as e:
            logging.error(f"Attempt {i + 1}: Failed to get data - {e}")
            time.sleep(2**i)
    
    return False, None

def extract_json_from_zip(zip_filepath, temp_dir):
    """Extract JSON from ZIP file to temporary directory, return json directory path"""
    
    try:
        # Create extraction temp directory within main temp dir
        extraction_temp_dir = os.path.join(temp_dir, 'extraction_temp')
        os.makedirs(extraction_temp_dir, exist_ok=True)
        logging.info(f'Extraction temp dir created: {extraction_temp_dir}')
        
        # Create JSON output directory within main temp dir
        json_dir = os.path.join(temp_dir, 'json')
        os.makedirs(json_dir, exist_ok=True)
        
        # Extract ZIP file
        with zipfile.ZipFile(zip_filepath, "r") as zip_ref:
            zip_ref.extractall(extraction_temp_dir)
        
        # Locate the day folder (should be a numeric folder name)
        day_folder = next(f for f in os.listdir(extraction_temp_dir) if f.isdigit())
        day_path = os.path.join(extraction_temp_dir, day_folder)
        
        # Unpack gzipped JSON files
        json_count = 0
        for root, _, files in os.walk(day_path):
            for file in files:
                if file.endswith(".gz"):
                    gz_path = os.path.join(root, file)
                    json_filename = file[:-3]  # Remove .gz extension
                    output_path = os.path.join(json_dir, json_filename)
                    
                    with gzip.open(gz_path, 'rb') as gz_file, open(output_path, 'wb') as out_file:
                        shutil.copyfileobj(gz_file, out_file)
                    json_count += 1
        
        logging.info(f"{json_count} JSON files extracted from {zip_filepath} to {json_dir}")
        
        # Clean up ZIP file
        os.remove(zip_filepath)
        logging.info(f"Deleted ZIP file: {zip_filepath}")
        
        return True, json_dir
        
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        return False, None

def upload_json_to_s3(json_dir, bucket, sub_dir, aws_access, aws_secret_access):
    """Upload JSON files from temporary directory to S3"""
    
    s3_client = boto3.client(
        service_name='s3',
        aws_access_key_id=aws_access,
        aws_secret_access_key=aws_secret_access
    )
    
    uploaded_cnt = 0
    
    for root, _, files in os.walk(json_dir):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                object_name = f"{sub_dir}/{file}"
                
                try:
                    s3_client.upload_file(file_path, bucket, object_name)
                    logging.info(f"File uploaded to S3: s3://{bucket}/{object_name}")
                    uploaded_cnt += 1
                except boto3.exceptions.S3UploadFailedError as e:
                    logging.error(f"S3 upload failed: {e}")
                except Exception as e:
                    logging.error(f"Unexpected error during S3 upload: {e}")
    
    logging.info(f"Total JSON files uploaded: {uploaded_cnt}")
    return uploaded_cnt

def list_s3_objects(bucket_name, prefix, aws_access, aws_secret):
    """List all files in S3 bucket with given prefix"""
    
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

def list_local_files(local_dir):
    """List local JSON files in directory"""
    
    file_paths = []
    
    for root, _, files in os.walk(local_dir):
        for name in files:
            if name.endswith('.json'):
                full_path = os.path.join(root, name)
                rel_path = os.path.relpath(full_path, local_dir).replace("\\", "/")
                file_paths.append(rel_path)
    
    return file_paths

def main():
    """Main function to orchestrate the entire process"""
    
    # Environment variables
    api_key = os.getenv("AMP_API_KEY")
    secret_key = os.getenv("AMP_SECRET_KEY")
    aws_access = os.getenv('ACCESS_KEY')
    aws_secret = os.getenv('SECRET_ACCESS_KEY')
    bucket_name = os.getenv("AWS_BUCKET_NAME")
    sub_dir = 'python_import'
    
    print(f"Bucket name: {bucket_name}")
    
    # Use context manager for temporary directory
    with temp_directory() as temp_dir:
        # Step 1: Export data from Amplitude API
        success, zip_filepath = export_api(start_time, end_time, api_key, secret_key, temp_dir)
        if not success:
            logging.error("Failed to export data from Amplitude API")
            return
        
        # Step 2: Extract JSON files from ZIP
        success, json_dir = extract_json_from_zip(zip_filepath, temp_dir)
        if not success:
            logging.error("Failed to extract JSON files from ZIP")
            return
        
        # Step 3: Upload JSON files to S3
        uploaded_count = upload_json_to_s3(json_dir, bucket_name, sub_dir, aws_access, aws_secret)
        
        # Step 4: Compare local and S3 files to find missing files
        local_files = list_local_files(json_dir)
        s3_files = list_s3_objects(bucket_name, sub_dir, aws_access, aws_secret)
        
        missing_files = [f for f in local_files if f not in s3_files]
        
        print(f"Local files: {len(local_files)}")
        print(f"S3 files: {len(s3_files)}")
        print(f"Missing files: {missing_files}")
        
        if missing_files:
            logging.warning(f"Missing files in S3: {missing_files}")
        else:
            logging.info("All files successfully uploaded to S3")

if __name__ == "__main__":
    main()