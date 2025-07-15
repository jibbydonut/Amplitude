from dotenv import load_dotenv
from amplitude import export_api
from helper import setup_logger
import datetime
import os

setup_logger()
load_dotenv()

api_key = os.getenv("AMP_API_KEY")
secret_key = os.getenv("AMP_SECRET_KEY")

end_time = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).strftime('%Y%m%dT23')
start_time = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).strftime('%Y%m%dT00')

export_api(start_time=start_time,end_time=end_time,api_key=api_key,secret_key=secret_key)