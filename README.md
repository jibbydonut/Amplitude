# Amplitude Export API Data Pipeline

A Python application that extracts event data from Amplitude's Export API and uploads it to AWS S3 for further processing and analysis.

## Overview

This tool automates the process of:
1. Fetching raw event data from Amplitude's Export API
2. Extracting compressed JSON files from the downloaded ZIP archive
3. Uploading the JSON files to an AWS S3 bucket
4. Verifying successful uploads by comparing local and remote file counts

## Features

- **Automated daily data extraction** from Amplitude (previous day's data)
- **Robust error handling** with retry logic for API requests
- **Temporary file management** with automatic cleanup
- **S3 integration** for scalable data storage
- **Upload verification** to ensure data integrity
- **Comprehensive logging** for monitoring and debugging

## Prerequisites

- Python 3.7+
- AWS account with S3 access
- Amplitude account with Export API access

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd amplitude-export-pipeline
```

2. Install required dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables by creating a `.env` file:
```env
# Amplitude API credentials
AMP_API_KEY=your_amplitude_api_key
AMP_SECRET_KEY=your_amplitude_secret_key

# AWS credentials
ACCESS_KEY=your_aws_access_key
SECRET_ACCESS_KEY=your_aws_secret_access_key
AWS_BUCKET_NAME=your_s3_bucket_name
```

## Dependencies

The application requires the following Python packages:
- `requests` - For HTTP API calls
- `boto3` - AWS SDK for S3 operations
- `python-dotenv` - Environment variable management
- Standard library modules: `os`, `datetime`, `logging`, `time`, `zipfile`, `gzip`, `shutil`, `tempfile`

## Usage

### Basic Usage

Run the main script to process the previous day's data:

```bash
python main.py
```

### What the Script Does

1. **Data Export**: Fetches compressed event data from Amplitude's Export API for the previous day (00:00 to 23:59 UTC)
2. **File Processing**: 
   - Downloads data as a ZIP file to a temporary directory
   - Extracts gzipped JSON files from the ZIP archive
   - Uncompresses JSON files for upload
3. **S3 Upload**: Uploads all JSON files to the specified S3 bucket under the `python_import/` prefix
4. **Verification**: Compares local files with S3 objects to ensure complete upload

### Configuration

The script uses the following default settings:
- **Time Range**: Previous day (00:00 to 23:59 UTC)
- **S3 Prefix**: `python_import/`
- **Retry Logic**: Up to 3 attempts for failed API requests
- **Temporary Files**: Automatically cleaned up after processing

## File Structure

```
amplitude-export-pipeline/
├── main.py                 # Main execution script
├── paste.txt              # Core data pipeline functions
├── modules/
│   └── helper.py          # Logging setup utilities
├── .env                   # Environment variables (not in repo)
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## API Endpoints

The script uses Amplitude's EU Export API endpoint:
- **Endpoint**: `https://analytics.eu.amplitude.com/api/2/export`
- **Authentication**: HTTP Basic Auth using API key and secret
- **Parameters**: 
  - `start`: Start time in format `YYYYMMDDTHH`
  - `end`: End time in format `YYYYMMDDTHH`

## Error Handling

The application includes comprehensive error handling for:
- **API Request Failures**: Exponential backoff retry logic
- **File I/O Errors**: Proper exception handling for file operations
- **S3 Upload Failures**: Individual file upload error tracking
- **Data Extraction Issues**: ZIP and GZIP extraction error handling

## Logging

The application provides detailed logging including:
- API request attempts and failures
- File processing progress
- S3 upload status
- Temporary directory management
- Upload verification results

## Security Considerations

- Store sensitive credentials in environment variables, never in code
- Use IAM roles with minimal required permissions for AWS access
- Regularly rotate API keys and access credentials
- Consider using AWS IAM roles instead of access keys when running on EC2

## Troubleshooting

### Common Issues

1. **Authentication Errors**: Verify Amplitude API credentials and AWS access keys
2. **S3 Permission Errors**: Ensure the AWS user has `s3:PutObject` permissions for the target bucket
3. **Network Timeouts**: Check internet connectivity and consider increasing retry delays
4. **File Extraction Errors**: Verify ZIP file integrity and available disk space

### Monitoring

Monitor the following for operational health:
- Log files for error patterns
- S3 bucket for expected daily uploads
- File count discrepancies between local processing and S3 uploads

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with appropriate tests
4. Submit a pull request with a clear description

## License

[Specify your license here]

## Support

For issues related to:
- **Amplitude API**: Consult [Amplitude's Export API documentation](https://amplitude.com/docs/apis/analytics/export)
- **AWS S3**: Refer to AWS S3 documentation
- **This Tool**: Create an issue in the repository