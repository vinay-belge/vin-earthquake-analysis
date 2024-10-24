import json
import requests
from google.cloud import storage
import logging

# Set up logging for utility functions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s'
)

def fetch_earthquake_data(api_url):
    """
    Fetch earthquake data from the USGS API.

    :param api_url: The USGS API endpoint URL.
    :return: JSON response containing earthquake data.
    :raises Exception: If the request to the API fails.
    """
    try:
        logging.info(f"Fetching earthquake data from {api_url}...")
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an error for bad responses
        logging.info(f"Successfully fetched data from {api_url}.")
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred while fetching data: {http_err}")
        raise
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Request error occurred: {req_err}")
        raise

def upload_to_gcs(bucket_name, data, destination_blob_name):
    """
    Upload raw JSON data to a Google Cloud Storage (GCS) bucket.

    :param bucket_name: The name of the GCS bucket.
    :param data: The raw JSON data to upload.
    :param destination_blob_name: The destination path (blob name) within the bucket.
    :raises Exception: If the upload to GCS fails.
    """
    try:
        logging.info(f"Uploading data to GCS bucket: {bucket_name}, blob: {destination_blob_name}...")
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        # Convert the data to a JSON string and upload to GCS
        blob.upload_from_string(json.dumps(data), content_type='application/json')
        logging.info(f"Data successfully uploaded to GCS: {destination_blob_name}.")
    except Exception as e:
        logging.error(f"Failed to upload data to GCS: {e}")
        raise

def check_if_file_exists_in_gcs(bucket_name, blob_name):
    """
    Check if a file already exists in the GCS bucket.

    :param bucket_name: The name of the GCS bucket.
    :param blob_name: The name of the blob (file) to check.
    :return: True if the file exists, False otherwise.
    :raises Exception: If there is an error during the process.
    """
    try:
        logging.info(f"Checking if file exists in GCS bucket: {bucket_name}, blob: {blob_name}...")
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        exists = blob.exists()
        if exists:
            logging.info(f"File {blob_name} exists in {bucket_name}.")
        else:
            logging.info(f"File {blob_name} does not exist in {bucket_name}.")
        return exists
    except Exception as e:
        logging.error(f"Error checking if file exists in GCS: {str(e)}")
        raise
