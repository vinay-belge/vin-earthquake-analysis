# main.py
from pyspark.sql import SparkSession
from common_utils import fetch_earthquake_data, upload_to_gcs
import os
from datetime import datetime
import logging

# Set up logging for the application
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set up Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\yoges\pythonProject\pythonProject\vin-earthquake-analysis\project-dipakraj-d3d73364818d.json"

# Initialize Spark session
spark = SparkSession.builder.appName('earthquake_data_ingestion').getOrCreate()

try:
    # Define the URL for historical earthquake data
    historical_url = r"https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
    bucket_name = "historical-data-bket"

    # Create landing location for the data in GCS
    landing_location = f"landing/{datetime.now().strftime('%Y%m%d')}_historical_earthquake_data.json"

    # Fetch historical data
    historical_data = fetch_earthquake_data(historical_url)
    logging.info("Historical data fetched successfully.")

    # Upload the fetched data to Google Cloud Storage
    upload_to_gcs(bucket_name, historical_data, landing_location)
    logging.info(f"Historical data uploaded to {bucket_name} successfully.")

except Exception as e:
    logging.error(f"An error occurred: {e}")

finally:
    # Stop the Spark session
    spark.stop()
