"""
***********************************************************************
************** Author:   Christian KEMGANG NGUESSOP *******************
************** Project:   datamart                  *******************
************** Version:  1.0.0                      *******************
***********************************************************************
"""

import os
import urllib.request
from minio import Minio
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Construct the endpoint URL from the environment variables
hostname = os.getenv("MINIO_HOSTNAME")
port = os.getenv("MINIO_PORT")
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")


def download_all_files():

    today = datetime.now()
    last_month = today.month - 1 if today.month > 1 else 12
    year = today.year if today.month > 1 else today.year - 1

    months_with_data = []  # Initialize month list with data
    # Fill in months_with_data according to the current month
    for month in range(1, today.month + 1):
        months_with_data.append(month)
    # print(f"Months with data: {months_with_data}")  # Display months with data

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    dest_dir = "../../data/raw/"  # File destination

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
        data_file = f"yellow_tripdata_{year}-{month:02d}.parquet"  # Get data file
        save_path = os.path.join(dest_dir, data_file)

        # Check if the file already exists locally
        if os.path.exists(save_path):
            print(f"File {data_file} already exists locally, skipping download.")
            continue  # Skip to the next file if it already exists

        if check_data_exists(year, month):
            file_url = f"{base_url}{data_file}"
            try:
                # Download the file and save it locally
                urllib.request.urlretrieve(file_url, save_path)
                print(f"Downloaded {data_file} to {save_path}")
                # upload_to_minio(data_file, save_path)  # Uncomment if needed
            except Exception as e:
                print(f"Failed to download {data_file}: {e}")


def download_single_file():

    today = datetime.now()
    last_month = today.month - 1 if today.month > 1 else 12
    year = today.year if today.month > 1 else today.year - 1

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    dest_dir = "../../data/raw/"  # File destination

    def check_data_exists(year, month):
        data_file = f"yellow_tripdata_{year}-{month:02d}.parquet"
        file_url = f"{base_url}{data_file}"
        try:
            # Send a HEAD request to check if the file exists
            response = urllib.request.urlopen(file_url)
            return response.status == 200
        except urllib.error.HTTPError:
            # print(f"The data for this month({month}) does not exist")
            return False

    # Search last month with data
    while not check_data_exists(year, last_month):
        if last_month == 1:
            last_month = 12
            year -= 1  # Decrement the year if we go back to December
        else:
            last_month -= 1  # Go to previous month

    data_file = (
        f"yellow_tripdata_{year}-{last_month:02d}.parquet"  # Get data of last month
    )
    # print(f"Use of data for: {data_file}")  # Display the data month

    os.makedirs(dest_dir, exist_ok=True)
    save_path = os.path.join(dest_dir, data_file)

    # Check if the file already exists locally
    if os.path.exists(save_path):
        print(f"File {data_file} already exists locally, skipping download.")
        return  # Exit the function if the file already exists

    file_url = f"{base_url}{data_file}"

    try:
        # Download the file and save it locally
        urllib.request.urlretrieve(file_url, save_path)
        print(f"Downloaded {data_file} to {save_path}")
        # upload_to_minio(data_file, save_path)  # Uncomment if needed to upload
    except Exception as e:
        print(f"Failed to download {data_file} : {e}")


def download_file_csv():
    base_url = " https://d37ci6vzurychx.cloudfront.net/misc/"
    dest_dir = "../../data/raw/"  # File destination
    data_file = f"taxi_zone_lookup.csv"
    file_url = f"{base_url}{data_file}"

    try:
        # Send a HEAD request to check if the file exists
        response = urllib.request.urlopen(file_url)

        if response.status == 200:
            os.makedirs(dest_dir, exist_ok=True)
            save_path = os.path.join(dest_dir, data_file)

            # Check if the file already exists locally
            if os.path.exists(save_path):
                print(f"File {data_file} already exists locally, skipping download.")
            else:
                try:
                    # Download the file and save it locally
                    urllib.request.urlretrieve(file_url, save_path)
                    print(f"Downloaded {data_file} to {save_path}")
                    # upload_to_minio(data_file, save_path)  # Uncomment if needed to upload
                except Exception as e:
                    print(f"Failed to download {data_file} : {e}")

    except urllib.error.HTTPError:
        print(f"The file {data_file} does not exist")


def write_data_minio():
    # Upload all the file to Minio.
    client = Minio(
        f"{hostname}:{port}", secure=False, access_key=access_key, secret_key=secret_key
    )
    bucket = "yellow-tripdata"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
        print(f"Bucket {bucket} created")
    else:
        print(f"Bucket {bucket} already exist")

    save_dir = "../../data/raw"
    for file_name in os.listdir(save_dir):
        if file_name.endswith(".parquet"):
            file_path = os.path.join(save_dir, file_name)
            try:
                client.fput_object(bucket, file_name, file_path)
                print(f"Uploaded {file_name} to the Minio {bucket} bucket")
            except Exception as e:
                print(f"Failed to download {file_name} to Minio: {e}")


def upload_to_minio(file_name, file_path):
    # Upload the file to Minio.
    client = Minio(
        f"{hostname}:{port}", secure=False, access_key=access_key, secret_key=secret_key
    )
    bucket = "taxi-data"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
        print(f"Bucket {bucket} created")
    else:
        print(f"Bucket {bucket} already exist")

    # Check if the file exists
    if file_name not in os.listdir(file_path):
        print(f"The {file_name} does not exists in the {file_path}")
        return

    # Check that the file is a .parquet file
    if file_name.endswith(".parquet"):
        full_file_path = os.path.join(file_path, file_name)
        try:
            # Upload file to Minio
            client.fput_object(bucket, file_name, full_file_path)
            print(f"Uploaded {file_name} to the Minio {bucket} bucket")
        except Exception as e:
            print(f"Failed to download {file_name} to Minio: {e}")
