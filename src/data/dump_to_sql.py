"""
***********************************************************************
************** Author:   Christian KEMGANG NGUESSOP *******************
************** Project:   datamart                  *******************
************** Version:  1.0.0                      *******************
***********************************************************************
"""

import os
import gc
import sys
from xmlrpc.client import ResponseError
import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine
from minio import Minio
from typing import List
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Construct the endpoint URL from the environment variables
hostname = os.getenv("MINIO_HOSTNAME")
port = os.getenv("MINIO_PORT")
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")

# Config warehouse
wh_dbms_username = os.getenv("WH_DBMS_USERNAME")
wh_dbms_password = os.getenv("WH_DBMS_PASSWORD")
wh_dbms_ip = os.getenv("WH_DBMS_IP")
wh_dbms_port = os.getenv("WH_DBMS_PORT")
wh_dbms_database = os.getenv("WH_DBMS_DATABASE")
wh_dbms_table = os.getenv("WH_DBMS_TABLE")


def write_data_postgres(dataframe: pd.DataFrame) -> bool:
    """
    Dumps a Dataframe to the DBMS engine

    Parameters:
        - dataframe (pd.Dataframe) : The dataframe to dump into the DBMS engine

    Returns:
        - bool : True if the connection to the DBMS and the dump to the DBMS is successful, False if either
        execution is failed
    """
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": f"{wh_dbms_username}",
        "dbms_password": f"{wh_dbms_password}",
        "dbms_ip": f"{wh_dbms_ip}",
        "dbms_port": f"{wh_dbms_port}",
        "dbms_database": f"{wh_dbms_database}",
        "dbms_table": f"{wh_dbms_table}",
    }

    # URL de connexion pour créer la base de données
    base_url = f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@{db_config['dbms_ip']}:{db_config['dbms_port']}/postgres"

    # Se connecter à la base de données "postgres" (base par défaut)
    try:
        conn = psycopg2.connect(base_url)
        conn.autocommit = True  # Nécessaire pour créer une base de données sans une transaction en cours
        cursor = conn.cursor()

        # Vérifier si la base de données existe
        cursor.execute(
            f"SELECT 1 FROM pg_database WHERE datname = '{db_config['dbms_database']}';"
        )
        if cursor.fetchone() is None:
            print(
                f"Database '{db_config['dbms_database']}' does not exist. Creating it..."
            )
            cursor.execute(f"CREATE DATABASE {db_config['dbms_database']};")
            print(f"Database '{db_config['dbms_database']}' created successfully.")
        else:
            print(f"Database '{db_config['dbms_database']}' already exists.")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error while checking/creating the database: {e}")
        return False

    # Connexion à la base de données "tp_warehouse" pour insérer les données
    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    try:
        # Connexion avec SQLAlchemy pour insérer les données
        engine = create_engine(db_config["database_url"])
        with engine.connect():
            success: bool = True
            print("Connection successful! Processing parquet file")
            dataframe.to_sql(
                db_config["dbms_table"], engine, index=False, if_exists="append"
            )

    except Exception as e:
        success: bool = False
        print(f"Error connection to the database: {e}")
        return success

    return success


def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Take a Dataframe and rewrite it columns into a lowercase format.
    Parameters:
        - dataframe (pd.DataFrame) : The dataframe columns to change

    Returns:
        - pd.Dataframe : The changed Dataframe into lowercase format
    """
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe


def get_parquet_files_from_minio(bucket_name: str, minio_client: Minio) -> List[str]:
    """
    Retrieve a list of Parquet files in a MinIO bucket.

    Parameters:
        - bucket_name (str): The name of the MinIO bucket
        - minio_client: The initialized Minio client

    Returns:
        - List[str]: A list of Parquet file names in the bucket
    """
    files = []
    try:
        # List all objects in the MinIO bucket
        objects = minio_client.list_objects(bucket_name, recursive=True)

        for obj in objects:
            if obj.object_name.lower().endswith(".parquet"):
                files.append(obj.object_name)
    except ResponseError as err:
        print(f"Error accessing MinIO bucket: {err}")

    return files


def download_parquet_from_minio(
    bucket_name: str, file_key: str, minio_client: Minio
) -> pd.DataFrame:
    """
    Download a Parquet file from MinIO and load it into a Pandas DataFrame.

    Parameters:
        - bucket_name (str): The MinIO bucket name
        - file_key (str): The key (path) of the file in the bucket
        - minio_client: The initialized Minio client

    Returns:
        - pd.DataFrame: The DataFrame containing the Parquet file data
    """
    try:
        # Download the object from MinIO using the correct parameter names
        obj = minio_client.get_object(bucket_name=bucket_name, object_name=file_key)

        # Read the object data into a DataFrame
        file_data = obj.read()
        return pd.read_parquet(BytesIO(file_data), engine="pyarrow")

    except Exception as e:
        print(f"Error downloading or reading the Parquet file: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error


# Recovers data from Minio and backups in postgres
def main() -> None:
    # MinIO configuration
    client = Minio(
        f"{hostname}:{port}", secure=False, access_key=access_key, secret_key=secret_key
    )

    bucket_name = "yellow-tripdata"

    # List Parquet files in the MinIO bucket
    parquet_files = get_parquet_files_from_minio(bucket_name, client)

    # Process each Parquet file
    for file_key in parquet_files:
        print(f"Processing file: {file_key}")

        # Download the Parquet file from MinIO
        parquet_df = download_parquet_from_minio(bucket_name, file_key, client)

        # Clean column names (convert to lowercase)
        clean_column_name(parquet_df)

        # Write the data to PostgreSQL
        if not write_data_postgres(parquet_df):
            del parquet_df
            gc.collect()
            return  # Stop processing on failure

        # Cleanup memory after each insertion
        del parquet_df
        gc.collect()


"""
# Retrieves data from a local directory and saves it in postgres
def main() -> None:
    # folder_path: str = r'..\..\data\raw'
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the relative path to the folder
    folder_path = os.path.join(script_dir, '..', '..', 'data', 'raw')

    parquet_files = [f for f in os.listdir(folder_path) if
                     f.lower().endswith('.parquet') and os.path.isfile(os.path.join(folder_path, f))]

    for parquet_file in parquet_files:
        parquet_df: pd.DataFrame = pd.read_parquet(os.path.join(folder_path, parquet_file), engine='pyarrow')

        clean_column_name(parquet_df)
        if not write_data_postgres(parquet_df):
            del parquet_df
            gc.collect()
            return

        del parquet_df
        gc.collect()
"""


if __name__ == "__main__":
    sys.exit(main())
