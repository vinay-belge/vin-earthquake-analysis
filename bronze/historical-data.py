from pyspark.sql import SparkSession
from common_utils import fetch_earthquake_data,upload_to_gcs
import os
from datetime import datetime


if __name__ == '__main__':

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\yoges\pythonProject\pythonProject\vin-earthquake-analysis\project-dipakraj-d3d73364818d.json"

    spark = SparkSession.builder.master('local[*]').appName('earthquake_data_ingestion').getOrCreate()

    historical_url = r"https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

    bucket_name = "historical-data-bket"
    landing_location = f"landing/{datetime.now().strftime('%Y%m%d')}historical_earthquake_data.json"

    historical_data = fetch_earthquake_data(historical_url)
    print("Historical data fetched successfully...!")

    upload_to_gcs(bucket_name,historical_data,landing_location)
    print(f"Historical data uploaded to {bucket_name} successfully...!")


