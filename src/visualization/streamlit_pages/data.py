"""
***********************************************************************
************** Author:   Christian KEMGANG NGUESSOP *******************
************** Project:   datamart                  *******************
************** Version:  1.0.0                      *******************
***********************************************************************
"""

import os
import streamlit as st
import psycopg2
import pandas as pd
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


# Fonction pour se connecter à PostgreSQL
def connect_to_db():
    try:
        # Connexion à PostgreSQL
        connection = psycopg2.connect(
            host=dm_dbms_ip,
            port=dm_dbms_port,
            user=dm_dbms_username,
            password=dm_dbms_password,
            dbname=dm_dbms_database,
        )
        return connection
    except Exception as e:
        st.error(f"Erreur de connexion à la base de données : {e}")
        return None


# Fonction pour obtenir le nombre total de lignes d'une table avec un cache pour éviter de charger tout le temps
@st.cache_data(ttl=86400)  # Cache pendant 24 heure (modifiable)
def get_table_row_count(table_name):
    conn = connect_to_db()
    if conn is None:
        return 0

    try:
        query = f"SELECT COUNT(*) FROM {table_name}"
        # Exécuter la requête pour obtenir le nombre de lignes
        result = pd.read_sql(query, conn)
        row_count = result.iloc[0, 0]  # Extraire le nombre de lignes de la réponse
        return row_count
    except Exception as e:
        st.error(
            f"Erreur lors de la récupération du nombre de lignes pour {table_name}: {e}"
        )
        return 0
    finally:
        conn.close()


# Fonction pour afficher une table spécifique avec un spinner de chargement et pagination avec un cache pour éviter de charger tout le temps
@st.cache_data(ttl=86400)  # Cache pendant 24 heure (modifiable)
def show_table(query, table_name, limit=200000):
    conn = connect_to_db()
    if conn is None:
        return

    try:
        # Afficher le spinner pendant que les données sont récupérées
        with st.spinner(f"Chargement des données de {table_name}..."):
            # Ajouter une clause LIMIT pour limiter le nombre de résultats
            query_with_limit = query + f" LIMIT {limit}"
            df = pd.read_sql(query_with_limit, conn)

        # Affichage des données dans Streamlit une fois le chargement terminé
        st.write(f"### {table_name}")
        st.write(df)

    except Exception as e:
        st.error(f"Erreur lors de la récupération des données pour {table_name}: {e}")
    finally:
        conn.close()


# Fonction principale pour afficher les différentes tables
def show_data():
    # Header de l'application
    st.title("📈 Data Taxi-Tech")

    st.write("Cette page permet d'afficher les données des taxis jaunes.")

    # Menu pour choisir quelle table afficher
    selected_table = st.selectbox(
        "Sélectionner une table à afficher :",
        [
            "dimension_payment",
            "dimension_time",
            "dimension_vendor",
            "dimension_zone",
            "fact_yellow_taxi",
        ],
        format_func=lambda x: x.replace("_", " "),  # Formatage dynamique des noms
    )

    # Récupérer le nombre total de lignes pour la table sélectionnée
    total_rows = get_table_row_count(selected_table)

    # Si aucune ligne n'est trouvée, afficher un message d'erreur
    if total_rows == 0:
        st.write(f"Aucune donnée trouvée dans la table {selected_table}.")
        return

    # Afficher un slider pour sélectionner le nombre de lignes à afficher, basé sur le nombre de lignes
    num_rows = st.slider(
        "Nombre de lignes à afficher",
        min_value=1,  # Valeur minimale
        max_value=10000,  # Valeur maximale basée sur le nombre total de lignes
        value=100,  # Valeur par défaut
        step=50,  # Incrément de 50 pour l'affichage des lignes
    )

    # Afficher les données en fonction de la table sélectionnée
    query = f"SELECT * FROM {selected_table}"

    # Formater le nom de la table (enlever les underscores et capitaliser)
    formatted_table_name = selected_table.replace("_", " ").title()

    show_table(query, formatted_table_name, limit=num_rows)
