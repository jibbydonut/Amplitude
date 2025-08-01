# 📊 Amplitude Data Export & S3 Upload Pipeline

This Python script automates the extraction of event data from Amplitude's Export API, decompresses the data, and uploads the resulting JSON files to an AWS S3 bucket. It includes robust error handling, logging, and file integrity checks to ensure reliable data ingestion.

---

## 🚀 Features

- Export hourly event data from Amplitude's EU API
- Extract and decompress `.zip` and `.gz` files into raw JSON
- Upload JSON files to a specified S3 bucket
- Compare local and S3 files to detect any upload failures
- Modular design with context-managed temporary directories
- Retry logic for API requests and detailed logging

---

## 📁 Project Structure

project/ │
├── main.py             # Main orchestration script 
├── modules/ │
   └── helper.py            # Contains setup_logger()
   └── amplitude.py            # main functions 
├── .env                     # Environment variables (API keys, AWS credentials)
├── README.md                # You're reading it!



---

## 🔧 Setup Instructions

### 1. Clone the Repository & Install requirements

git clone https://github.com/your-repo/amplitude-export-s3.git
cd amplitude-export-s3

pip install -r requirements.txt

### 3. Configure Environment Variables
Create a .env file in the root directory with the following keys

AMP_API_KEY=your_amplitude_api_key
AMP_SECRET_KEY=your_amplitude_secret_key

ACCESS_KEY=your_aws_access_key
SECRET_ACCESS_KEY=your_aws_secret_key
AWS_BUCKET_NAME=your_s3_bucket_name

### Script Details

The script will:
- Export data for the previous UTC day from Amplitude
- Extract and decompress the data into JSON files
- Upload the JSON files to your specified S3 bucket under the python_import/ prefix
- Log and print any missing files that failed to uploa

Notes
- The script targets Amplitude's EU endpoint (https://analytics.eu.amplitude.com/api/2/export). Update the URL if you're using the US region.
- Data is pulled for the previous UTC day (00 to 23 hour range).
- Temporary directories are automatically cleaned up after execution.
- Logging is configured via setup_logger() in modules/helper.py

