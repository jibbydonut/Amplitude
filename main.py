from dotenv import load_dotenv
from modules.amplitude import export_api,extract_json_from_zip,upload_json_to_s3
from modules.helper import setup_logger
import logging
import datetime
import os

setup_logger()
load_dotenv()

api_key = os.getenv("AMP_API_KEY")
secret_key = os.getenv("AMP_SECRET_KEY")

file_path = "local/json"

aws_access = os.getenv('ACCESS_KEY')
aws_secret = os.getenv('SECRET_ACCESS_KEY')
bucket = os.getenv("AWS_BUCKET_NAME")
sub_dir = 'python_import'

end_time = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).strftime('%Y%m%dT23')
start_time = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).strftime('%Y%m%dT00')

# export_api(start_time=start_time,end_time=end_time,api_key=api_key,secret_key=secret_key)

if export_api(start_time=start_time,end_time=end_time,api_key=api_key,secret_key=secret_key):
    if extract_json_from_zip(start_time=start_time,end_time=end_time):
        upload_json_to_s3(file_path=file_path, bucket=bucket,sub_dir=sub_dir,aws_access=aws_access,aws_secret_access=aws_secret)
    else:
        logging.warning('Skipping S3 Upload because extract_json_from_zip() failed')
else:
     logging.warning('Skipping JSON extraction because export_api() failed.')



