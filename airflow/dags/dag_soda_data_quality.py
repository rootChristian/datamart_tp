"""
***********************************************************************
************** Author:   Christian KEMGANG NGUESSOP *******************
************** Project:   datamart                  *******************
************** Version:  1.0.0                      *******************
***********************************************************************
"""

import os
import logging
import psycopg2
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

# Configuration du logging
logging.basicConfig(level=logging.INFO)

# Variables d'environnement pour la connexion à la base de données
dm_dbms_username = os.getenv("DOCKER_DBMS_USERNAME")
dm_dbms_password = os.getenv("DOCKER_DBMS_PASSWORD")
dm_dbms_ip = os.getenv("DOCKER_DBMS_IP")
dm_dbms_port = os.getenv("DOCKER_DBMS_PORT")
dm_dbms_database = os.getenv("DOCKER_DBMS_DATABASE")
dm_dbms_datasource = os.getenv("DOCKER_DBMS_DATASOURCE")

# Liste des tables à vérifier
table_tasks = [
    "dimension_vendor",
    "dimension_time",
    "dimension_zone",
    "dimension_payment",
    "fact_yellow_taxi",
]

# Récupérer le chemin absolu du répertoire où se trouve ce fichier
dag_dir = os.path.dirname(os.path.abspath(__file__))

# Reculer d'un cran pour atteindre le répertoire parent, puis accéder au répertoire "soda"
soda_dir = os.path.join(dag_dir, "..", "soda")

# Normaliser le chemin pour s'assurer qu'il est valide
soda_dir = os.path.abspath(soda_dir)  # soda_directory = "/opt/airflow/soda"
checks_path = os.path.join(
    soda_dir, "checks"
)  # checks_directory = "/opt/airflow/soda/checks"


# Fonction pour vérifier l'existence de la base de données
def check_database_exists():
    try:
        conn = psycopg2.connect(
            host=dm_dbms_ip,
            port=dm_dbms_port,
            user=dm_dbms_username,
            password=dm_dbms_password,
            dbname=dm_dbms_database,
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        logging.info("La base de données est accessible.")
        conn.close()
        return True
    except Exception as e:
        logging.error(f"Erreur lors de la connexion à la base de données : {e}")
        return False


# Fonction pour vérifier l'existence des tables spécifiques dans la base de données
def check_tables_exists():
    try:
        conn = psycopg2.connect(
            host=dm_dbms_ip,
            port=dm_dbms_port,
            user=dm_dbms_username,
            password=dm_dbms_password,
            dbname=dm_dbms_database,
        )
        cursor = conn.cursor()

        # Vérifier l'existence de chaque table
        for table in table_tasks:
            cursor.execute(f"SELECT to_regclass('public.{table}')")
            result = cursor.fetchone()
            if result[0] is None:
                logging.error(f"Table {table} n'existe pas.")
                conn.close()
                return False
        conn.close()
        return True
    except Exception as e:
        logging.error(f"Erreur lors de la vérification des tables : {e}")
        return False


# Fonction de branchement pour décider si on exécute les vérifications de qualité des données
def choose_next_task(**kwargs):
    if not check_database_exists():
        logging.error("La base de données n'existe pas.")
        return "no_database_or_tables"

    if not check_tables_exists():
        logging.error("Certaines tables sont manquantes.")
        return "no_database_or_tables"

    return "soda_quality_check_connection"


# Vérification de l'existence des fichiers Soda avant d'exécuter les étapes suivantes
def verify_soda_files():
    missing_files = []

    if not os.path.exists(os.path.join(soda_dir, "configuration.yml")):
        missing_files.append("configuration.yml")
    if not os.path.exists(os.path.join(soda_dir, "checks")):
        missing_files.append("checks")
    if not os.path.exists(os.path.join(soda_dir, "reports")):
        missing_files.append("reports")

    # Vérifier la présence des fichiers de vérification pour chaque table
    for table in table_tasks:
        check_file = f"{table}_check.yml"
        check_file_path = os.path.join(checks_path, check_file)
        if not os.path.exists(check_file_path):
            missing_files.append(check_file)

    if missing_files:
        missing_files_str = ", ".join(missing_files)
        raise FileNotFoundError(
            f"Les fichiers suivants sont manquants : {missing_files_str}"
        )
    logging.info("Tous les fichiers de configuration Soda sont présents.")


# Création du DAG
with DAG(
    "soda_quality_checks_with_db_verification",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="Vérification de la base de données et des tables avant les contrôles de qualité avec Soda",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 11),
    catchup=False,
) as dag:

    # Tâche pour vérifier l'existence des fichiers Soda
    verify_files = PythonOperator(
        task_id="verify_soda_files",
        python_callable=verify_soda_files,
        dag=dag,
    )

    # Tâche de branchement pour décider de la suite
    branching_task = BranchPythonOperator(
        task_id="choose_next_task",
        python_callable=choose_next_task,
        provide_context=True,
        dag=dag,
    )

    soda_quality_check_connection = BashOperator(
        task_id="soda_quality_check_connection",
        bash_command=f"soda test-connection -d {dm_dbms_datasource} -c {soda_dir}/configuration.yml",
        dag=dag,
    )

    # Liste pour stocker les tâches de qualité
    soda_quality_check_tasks = []

    # Créer une tâche `BashOperator` pour chaque élément de `tasks`
    for i, task_file in enumerate(table_tasks):
        task_id = f"task_{i}_soda_quality_check"  # ID unique pour chaque tâche
        bash_command = f"soda scan -d {dm_dbms_datasource} -c {soda_dir}/configuration.yml {checks_path}/{task_file}_check.yml"

        soda_quality_check_task = BashOperator(
            task_id=task_id,
            bash_command=bash_command,
            dag=dag,
        )

        # Ajouter chaque tâche à la liste
        soda_quality_check_tasks.append(soda_quality_check_task)

    # Tâche pour indiquer l'absence de base de données ou de tables
    no_database_or_tables = BashOperator(
        task_id="no_database_or_tables",
        bash_command="echo 'La base de données ou les tables sont manquantes, vérifiez la configuration.'",
        dag=dag,
    )

    # Définir les dépendances
    verify_files >> branching_task
    branching_task >> [no_database_or_tables, soda_quality_check_connection]
    for soda_quality_check_task in soda_quality_check_tasks:
        (soda_quality_check_connection >> soda_quality_check_task)
