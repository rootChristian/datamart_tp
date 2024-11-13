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
            #print(f"The data for this month({month}) does not exist")
            return False

    # Search last month with data
    while not check_data_exists(year, last_month):
        if last_month == 1:
            last_month = 12
            year -= 1   # Decrement the year if we go back to December
        else:
            last_month -= 1 # Go to previous month
    
    data_file = f"yellow_tripdata_{year}-{last_month:02d}.parquet"  # Get data of last month
    #print(f"Use of data for: {data_file}")  # Display the data month

    os.makedirs(dest_dir, exist_ok=True)
    file_url = f"{base_url}{data_file}"
    save_path = os.path.join(dest_dir, data_file)

    try:
        # Download the file and save it locally
        urllib.request.urlretrieve(file_url, save_path)
        print(f"Download {data_file} in {save_path}")
        #upload_to_minio(data_file, save_path)
    except Exception as e:
        print(f"Failed to download {data_file} : {e}")

def write_data_minio():
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket = "NOM_DU_BUCKET_ICI"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
        print(f"Bucket {bucket} created")
    else:
        print(f"Bucket {bucket} already exist")
    
    save_dir = "../../data/raw"
    for file_name in os.listdir(save_dir):
        if file_name.endswith('.parquet'):
            file_path = os.path.join(save_dir, file_name)
            try:
                client.fput_object(bucket, file_name, file_path)
                print(f"Téléchargé {file_name} dans le bucket Minio {bucket}")
            except Exception as e:
                print(f"Échec du téléchargement de {file_name} : {e}")

def upload_to_minio(file_name, file_path):
    # Download the file into Minio.
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket = "taxi-data"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
        print(f"Bucket {bucket} créé")
    
    try:
        client.fput_object(bucket, file_name, file_path)
        print(f"Téléchargé {file_name} dans le bucket Minio {bucket}")
    except Exception as e:
        print(f"Échec du téléchargement de {file_name} dans Minio : {e}")

if __name__ == '__main__':
    sys.exit(main())
