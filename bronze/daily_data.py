import os
from datetime import datetime
from pyspark.sql import SparkSession
from common_utils import fetch_earthquake_data, upload_to_gcs, check_if_file_exists_in_gcs
import logging

# Set up logging for the main script
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s'
)

# Path to your GCP service account key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\yoges\pythonProject\pythonProject\vin-earthquake-analysis\project-dipakraj-d3d73364818d.json"

# Initialize Spark session for data processing
spark = SparkSession.builder.master('local[*]').appName('earthquake_data_ingestion').getOrCreate()

# URL to fetch daily earthquake data
daily_url = r"https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

# Bucket and file details for GCS
bucket_name = "daily-data-bket"
file_name = f"{datetime.now().strftime('%Y-%m-%d')}_daily_earthquake_data.json"
landing_location = f"landing/{file_name}"

logging.info("Starting the process of fetching daily earthquake data...")

# Fetch earthquake data
daily_data = fetch_earthquake_data(daily_url)

# Check if the file already exists in GCS
if check_if_file_exists_in_gcs(bucket_name, landing_location):
    logging.info(f"File {file_name} already exists in GCS. It will be replaced.")

# Upload data to GCS (new file or replace existing)
upload_to_gcs(bucket_name, daily_data, landing_location)
logging.info(f"Data uploaded to {bucket_name} successfully at {landing_location}.")

