"""
***********************************************************************
************** Author:   Christian KEMGANG NGUESSOP *******************
************** Project:   datamart                  *******************
************** Version:  1.0.0                      *******************
***********************************************************************
"""

import psycopg2
from sqlalchemy import create_engine
import os
from data_function import download_file_csv
import pandas as pd
from psycopg2 import sql
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Construct the endpoint URL from the environment variables
# Config datamart
dm_dbms_username = os.getenv("DM_DBMS_USERNAME")
dm_dbms_password = os.getenv("DM_DBMS_PASSWORD")
dm_dbms_ip = os.getenv("DM_DBMS_IP")
dm_dbms_port = os.getenv("DM_DBMS_PORT")
dm_dbms_database = os.getenv("DM_DBMS_DATABASE")


def execute_sql_script(conn, script_path):
    """
    Executes a given SQL script on the provided connection.

    Args:
        conn (psycopg2.connection): Active PostgreSQL connection.
        script_path (str): Path to the SQL script file.

    Returns:
        bool: True if script executed successfully, False otherwise.
    """
    try:
        with open(script_path, "r") as file:
            sql = file.read()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()  # Ensure changes are saved to the database
        cursor.close()
        return True
    except Exception as e:
        print(f"Error executing SQL script {script_path}: {e}")
        return False


def create_datamart_olap() -> bool:
    """
    Create the Data Mart OLAP schema and tables by executing creation.sql and insertion.sql.

    Returns:
        bool: True if the creation and insertion were successful, False otherwise.
    """
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": f"{dm_dbms_username}",
        "dbms_password": f"{dm_dbms_password}",
        "dbms_ip": f"{dm_dbms_ip}",
        "dbms_port": f"{dm_dbms_port}",
        "dbms_database": f"{dm_dbms_database}",
    }

    # URL for connecting to the OLAP data mart database
    base_url = f"postgresql://{db_config['dbms_username']}:{db_config['dbms_password']}@{db_config['dbms_ip']}:{db_config['dbms_port']}/postgres"

    try:
        # Connect to PostgreSQL (default 'postgres' database)
        conn = psycopg2.connect(base_url)
        conn.autocommit = True

        # Check if the Data Mart OLAP database exists
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT 1 FROM pg_database WHERE datname = '{db_config['dbms_database']}';"
        )
        if cursor.fetchone() is None:
            print(f"Creating database '{db_config['dbms_database']}'...")
            cursor.execute(f"CREATE DATABASE {db_config['dbms_database']};")
            print(f"Database '{db_config['dbms_database']}' created successfully.")

        cursor.close()  # Close the cursor after checking database existence

        # Reconnect to the OLAP Data Mart database
        db_config["database_url"] = (
            f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
            f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
        )
        conn = psycopg2.connect(db_config["database_url"])
        conn.autocommit = True

        # Path to SQL scripts
        creation_script_path = os.path.join(os.getcwd(), "creation.sql")
        insertion_script_path = os.path.join(os.getcwd(), "insertion.sql")

        # Execute the creation SQL script
        if not execute_sql_script(conn, creation_script_path):
            print("Error executing creation script.")
            return False
        else:
            print("Tables created successfully.")

        # Uncomment this block if you want to execute the insertion SQL script as well
        """
        if not execute_sql_script(conn, insertion_script_path):
            print("Error executing insertion script.")
            return False
        else:
            print("Tables populated successfully.")
        """

        print("Operation Data Mart OLAP completed successfully.")
        conn.close()
        return True

    except Exception as e:
        print(f"Error while creating or connecting to the OLAP Data Mart: {e}")
        return False


# Fonction pour se connecter à PostgreSQL
def connect_to_db():
    try:
        # Connexion à PostgreSQL, remplacez les valeurs par vos paramètres
        connection = psycopg2.connect(
            host=dm_dbms_ip,
            port=dm_dbms_port,
            user=dm_dbms_username,
            password=dm_dbms_password,
            dbname=dm_dbms_database,
        )
        return connection
    except Exception as e:
        print(f"Erreur de connexion à la base de données : {e}")
        return None


# Fonction pour insérer les données dans la table PostgreSQL
def insert_data_from_csv():
    # Lire les données CSV avec pandas
    try:
        # Chemin vers votre fichier CSV
        dest_dir = "../../data/raw/"  # File destination
        data_file = f"taxi_zone_lookup.csv"
        os.makedirs(dest_dir, exist_ok=True)
        csv_file_path = os.path.join(dest_dir, data_file)

        df = pd.read_csv(csv_file_path)
    except Exception as e:
        print(f"Erreur de lecture du fichier CSV : {e}")
        return
    # Connexion à la base de données
    connection = connect_to_db()
    if connection is None:
        return

    cursor = connection.cursor()

    # Préparer la requête d'insertion SQL
    insert_query = sql.SQL(
        """
        INSERT INTO dimension_zone (id_zone, borough, name_zone, service_zone)
        VALUES (%s, %s, %s, %s)
    """
    )

    try:
        # Insérer les lignes du DataFrame dans la base de données
        for index, row in df.iterrows():
            cursor.execute(
                insert_query,
                (row["LocationID"], row["Borough"], row["Zone"], row["service_zone"]),
            )

        # Commit des modifications
        connection.commit()
        print(f"{len(df)} lignes insérées avec succès.")

    except Exception as e:
        print(f"Erreur lors de l'insertion des données : {e}")
        connection.rollback()  # Annule les changements en cas d'erreur

    finally:
        # Fermeture de la connexion à la base de données
        cursor.close()
        connection.close()


# Fonction principale
def main() -> None:
    """
    Download a single file csv in local
    """
    # download_file_csv()

    """
    Main function to create the Data Mart OLAP, read data from the Data Warehouse,
    and insert it into the OLAP Data Mart.
    """
    # create_datamart_olap()

    """
        Insert data form csv
    """
    # insert_data_from_csv()


# Call the main function to start the process
if __name__ == "__main__":
    main()
