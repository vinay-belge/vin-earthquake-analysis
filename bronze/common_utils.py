import json
import requests
from google.cloud import storage

# function to fetch the historical data from USGS earthquake API
def fetch_earthquake_data(api_url):
    """
    Fetch earthquake data from the USGS API

    :param api_url: The USGS API endpoint url.
    :return: JSON response file containing earthquake data.
    """
    response = requests.get(api_url)
    if response.status_code==200:
        return response.json()
    else:
        raise Exception(f"failed to fetch data from {api_url}")

def upload_to_gcs(bucket_name, data, destination_blob_name):
    """
    Upload raw JSON data to a GCS bucket.
    :param bucket_name: The name of the GCS bucket.
    :param data: The raw JSON data to upload.
    :param destination_blob_name: The destination path (blob name) within the bucket.
    """
    # Initialize the GCS client using the credentials
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Convert the data to a JSON string and upload to GCS
    blob.upload_from_string(json.dumps(data), content_type='application/json')
    print(f"Data uploaded to GCS: {destination_blob_name}")













