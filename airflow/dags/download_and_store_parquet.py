"""
***********************************************************************
************** Author:   Christian KEMGANG NGUESSOP *******************
************** Project:   datamart                  *******************
************** Version:  1.0.0                      *******************
***********************************************************************
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import urllib.request
from io import BytesIO
import urllib.error
from minio import Minio
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Construct the endpoint URL from the environment variables
hostname = os.getenv("MINIO_HOSTNAME")
port = os.getenv("MINIO_PORT")
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")


def download_single_file():
    today = datetime.now()
    last_month = today.month - 1 if today.month > 1 else 12
    year = today.year if today.month > 1 else today.year - 1

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    def check_data_exists(year, month):
        name_parquet = f"yellow_tripdata_{year}-{month:02d}.parquet"
        file_url = f"{base_url}{name_parquet}"
        try:
            # Send a HEAD request to check if the file exists
            response = urllib.request.urlopen(file_url)
            return response.status == 200
        except urllib.error.HTTPError:
            return False

    # Search last month with data
    while not check_data_exists(year, last_month):
        if last_month == 1:
            last_month = 12
            year -= 1  # Decrement the year if we go back to December
        else:
            last_month -= 1  # Go to previous month

    name_parquet = f"yellow_tripdata_{year}-{last_month:02d}.parquet"

    # Téléchargement du fichier depuis l'URL
    file_url = f"{base_url}{name_parquet}"
    try:
        response = urllib.request.urlopen(file_url)
        file_data = BytesIO(
            response.read()
        )  # Charger le fichier dans la mémoire (en BytesIO)
        return (
            name_parquet,
            file_data,
        )  # Retourne le nom du fichier et l'objet BytesIO contenant le fichier
    except Exception as e:
        print(f"Erreur lors du téléchargement du fichier {name_parquet} : {e}")
        return None, None  # Retourne None si le téléchargement échoue


def download_and_store_parquet(**kwargs):
    client = Minio(
        f"{hostname}:{port}", secure=False, access_key=access_key, secret_key=secret_key
    )
    bucket = "taxi-data"

    # Vérifier si le bucket existe, et le créer si nécessaire
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Bucket {bucket} created")
    else:
        print(f"Bucket {bucket} already exists")

    # Récupérer le fichier du mois dernier
    name_parquet, file_data = download_single_file()

    if file_data is None:
        print("Échec du téléchargement du fichier.")
        return

    try:
        # Vérifier si le fichier existe déjà dans Minio
        # Utilisation de list_objects pour vérifier l'existence du fichier
        objects = client.list_objects(bucket, prefix=name_parquet, recursive=True)
        if any(obj.object_name == name_parquet for obj in objects):
            print(
                f"Le fichier {name_parquet} existe déjà dans Minio. Téléchargement ignoré."
            )
            return

        # Télécharger le fichier dans Minio depuis l'objet en mémoire
        file_data.seek(0)  # Réinitialiser le pointeur du fichier à la première position
        client.put_object(bucket, name_parquet, file_data, len(file_data.getvalue()))
        print(f"Fichier {name_parquet} téléchargé et stocké dans le bucket {bucket}")
    except Exception as e:
        print(
            f"Erreur lors du téléchargement ou de l'upload du fichier {name_parquet}: {e}"
        )


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 9),
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

# Définition du DAG
with DAG(
    "download_parquet_dag",
    default_args=default_args,
    description="Télécharger un parquet et le stocker dans Minio",
    schedule_interval="@monthly",  # Planifier pour s'exécuter chaque mois
    catchup=False,  # Set True to run now
) as dag:

    download_task = PythonOperator(
        task_id="download_and_store_parquet",
        python_callable=download_and_store_parquet,
    )
