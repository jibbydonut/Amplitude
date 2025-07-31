from dotenv import load_dotenv
from modules.amplitude import export_api, extract_json_from_zip, upload_json_to_s3, temp_directory
from modules.helper import setup_logger
import logging
import datetime
import os

setup_logger()
load_dotenv()

# Environment variables
api_key = os.getenv("AMP_API_KEY")
secret_key = os.getenv("AMP_SECRET_KEY")
aws_access = os.getenv('ACCESS_KEY')
aws_secret = os.getenv('SECRET_ACCESS_KEY')
bucket = os.getenv("AWS_BUCKET_NAME")
sub_dir = 'python_import'

# Time parameters
end_time = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).strftime('%Y%m%dT23')
start_time = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).strftime('%Y%m%dT00')

# Use context manager for temporary directory
with temp_directory() as temp_dir:
    logging.info(f"Starting Amplitude data export process for {start_time} to {end_time}")
    
    # Step 1: Export data from Amplitude API
    success, zip_filepath = export_api(
        start_time=start_time,
        end_time=end_time,
        api_key=api_key,
        secret_key=secret_key,
        temp_dir=temp_dir
    )
    
    if success:
        logging.info("Amplitude API export successful")
        
        # Step 2: Extract JSON files from ZIP
        success, json_dir = extract_json_from_zip(zip_filepath, temp_dir)
        
        if success:
            logging.info("JSON extraction successful")
            
            # Step 3: Upload JSON files to S3
            uploaded_count = upload_json_to_s3(
                json_dir=json_dir,
                bucket=bucket,
                sub_dir=sub_dir,
                aws_access=aws_access,
                aws_secret_access=aws_secret
            )
            
            logging.info(f"Process completed successfully. {uploaded_count} files uploaded to S3.")
            
        else:
            logging.error('Skipping S3 Upload because extract_json_from_zip() failed')
    else:
        logging.error('Skipping JSON extraction because export_api() failed.')

logging.info("Process finished. Temporary files cleaned up automatically.")