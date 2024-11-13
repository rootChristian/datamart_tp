"""
***********************************************************************
************** Author:   Christian KEMGANG NGUESSOP *******************
************** Project:   datamart                  *******************
************** Version:  1.0.0                      *******************
***********************************************************************
"""

from minio import Minio
import urllib.request
#import pandas as pd
import os
import sys
from datetime import datetime, timedelta

def main():
    grab_data()

def grab_data() -> None:
    
    today = datetime.now()
    last_month = today.month - 1 if today.month > 1 else 12
    year = today.year if today.month > 1 else today.year - 1

    months_with_data = []   # Initialize month list with data
    # Fill in months_with_data according to the current month
    for month in range(1, today.month + 1):
        months_with_data.append(month)
    #print(f"Months with data: {months_with_data}")  # Display months with data

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    dest_dir = "../../data/raw/" # File destination

    def check_data_exists(year, month):
        data_file = f"yellow_tripdata_{year}-{month:02d}.parquet"
        file_url = f"{base_url}{data_file}"
        try:
            # Send a HEAD request to check if the file exists
            response = urllib.request.urlopen(file_url)
            return response.status == 200
        except urllib.error.HTTPError:
            print(f"The data for this month({month}) does not exist")
            return False
        
    os.makedirs(dest_dir, exist_ok=True)

    # Download all existing data for the months in months_with_data
    for month in months_with_data:
        if check_data_exists(year, month):
            data_file = f"yellow_tripdata_{year}-{month:02d}.parquet"  # Get data file
            file_url = f"{base_url}{data_file}"
            save_path = os.path.join(dest_dir, data_file)

            try:
                # Download the file and save it locally
                urllib.request.urlretrieve(file_url, save_path)
                print(f"Download {data_file} in {save_path}")
                # upload_to_minio(data_file, save_path)  # Uncomment if needed
            except Exception as e:
                print(f"Failed to download {data_file} : {e}")
                

if __name__ == '__main__':
    sys.exit(main())
