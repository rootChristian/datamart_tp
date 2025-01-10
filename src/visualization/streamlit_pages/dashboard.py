"""
***********************************************************************
************** Author:   Christian KEMGANG NGUESSOP *******************
************** Project:   datamart                  *******************
************** Version:  1.0.0                      *******************
***********************************************************************
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine
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

# URL de connexion dynamique avec les variables d'environnement
db_url = f"postgresql://{dm_dbms_username}:{dm_dbms_password}@{dm_dbms_ip}:{dm_dbms_port}/{dm_dbms_database}"


# Charger les données depuis PostgreSQL avec un cache pour éviter de charger tout le temps
@st.cache_data(ttl=86400)  # Cache pendant 24 heure (modifiable)
def load_data():
    engine = create_engine(db_url)  # Connexion à la base de données avec SQLAlchemy
    query_fact = """
        SELECT v.vendor_name, COALESCE(tp.month, td.month) as month, COALESCE(tp.week, td.week) as week, 
               COALESCE(tp.day, td.day) as day, COALESCE(tp.hour, td.hour) as hour, zp.name_zone as zone_pickup, 
               zd.name_zone as zone_dropoff, f.total_amount , p.payment_method
        FROM fact_yellow_taxi f
        JOIN dimension_payment p ON f.id_payment_type = p.id_payment_type
        JOIN dimension_time tp ON f.id_time_pickup = tp.id_time
        JOIN dimension_time td ON f.id_time_dropoff = td.id_time
        JOIN dimension_vendor v ON f.id_vendor = v.id_vendor
        JOIN dimension_zone zp ON f.id_zone_pickup = zp.id_zone
        JOIN dimension_zone zd ON f.id_zone_dropoff = zd.id_zone
        LIMIT 100
    """
    return pd.read_sql(query_fact, engine)


# Prétraitement des données pour les analyses (mappage des mois, jours, etc.)
def preprocess_data(df):
    months_dict = {
        1: "Jan",
        2: "Fév",
        3: "Mar",
        4: "Avr",
        5: "Mai",
        6: "Juin",
        7: "Juil",
        8: "Aoû",
        9: "Sep",
        10: "Oct",
        11: "Nov",
        12: "Déc",
    }
    days_dict = {1: "Lun", 2: "Mar", 3: "Mer", 4: "Jeu", 5: "Ven", 6: "Sam", 7: "Dim"}
    hours_dict = {i: f"{i}h" for i in range(24)}

    df["month"] = df["month"].map(months_dict)
    df["week"] = df["week"].map(days_dict)
    df["hour"] = df["hour"].map(hours_dict)

    # Groupement des données pour des analyses plus rapides
    df_month = df.groupby("month").size().reset_index(name="Nombre de trajets")
    df_week = df.groupby("week").size().reset_index(name="Nombre de trajets")
    df_day = df.groupby("day").size().reset_index(name="Nombre de trajets")
    df_hour = df.groupby("hour").size().reset_index(name="Nombre de trajets")

    # Assurez-vous que les mois, jours, heures sont bien ordonnés
    df_month["month"] = pd.Categorical(
        df_month["month"], categories=months_dict.values(), ordered=True
    )
    df_week["week"] = pd.Categorical(
        df_week["week"], categories=days_dict.values(), ordered=True
    )
    df_hour["hour"] = pd.Categorical(
        df_hour["hour"], categories=[f"{i}h" for i in range(24)], ordered=True
    )

    # Trier les DataFrames par les colonnes catégorielles
    df_month = df_month.sort_values("month")
    df_week = df_week.sort_values("week")
    df_hour = df_hour.sort_values("hour")

    return df, df_month, df_week, df_day, df_hour


# Fonction de création de graphiques
def create_bar_chart(data, x_col, y_col, title, x_title, y_title, color=None):
    """
    Crée un graphique à barres verticales avec Plotly.
    """
    # Si color est passé, on l'utilise pour colorier les barres, sinon on les colore d'une couleur fixe
    if color:
        fig = px.bar(
            data,
            x=x_col,
            y=y_col,
            labels={x_col: x_title, y_col: y_title},
            title=title,
            color=color,  # Utilisation de la colonne spécifiée pour la couleur
            color_continuous_scale="Viridis",  # Choix du dégradé de couleurs
        )
    else:
        fig = px.bar(
            data,
            x=x_col,
            y=y_col,
            labels={x_col: x_title, y_col: y_title},
            title=title,
            # color_discrete_sequence=["skyblue"],  # Choisit une couleur fixe si color n'est pas passé
        )

    # Mise à jour de la mise en page
    fig.update_layout(
        title_font=dict(family="Arial", size=24, color="black"),
        template="none",  # Choisir le style de la mise en page
        showlegend=True,  # Pas besoin de légende ici
        xaxis_title=x_title,  # Titre de l'axe X
        yaxis_title=y_title,  # Titre de l'axe Y
        margin=dict(
            l=80, r=40, t=40, b=90
        ),  # Ajuste les marges pour un meilleur espacement
    )

    return fig


# Fonction pour afficher le dashboard
def show_dashboard():
    # Affichage du titre
    st.title("📊 Dashboard Taxi-Tech")

    # Chargement et prétraitement des données
    df = load_data()
    df, df_month, df_week, df_day, df_hour = preprocess_data(df)

    # Calculs des indicateurs principaux
    total_amount = pd.to_numeric(df["total_amount"], errors="coerce").sum()
    nombre_trajets_annules = len(df[df["total_amount"] == 0])
    nombre_total_trajets = len(df)
    pourcentage_interruption = (nombre_trajets_annules / nombre_total_trajets) * 100

    # Affichage des résultats principaux
    data, total, pourcentage = st.columns([4, 1, 1])

    with data:
        st.write(df.head())
    with total:
        st.info("Montant total des trajets", icon="💰")
        st.metric(label="Somme totale", value=f"{total_amount:,.0f}$")
    with pourcentage:
        st.info("Pourcentage du nombre de trajets annulés", icon="💯")
        st.metric(
            label="Pourcentage d'interruption", value=f"{pourcentage_interruption:.2f}%"
        )

    st.markdown(""" --- """)

    # Sidebar pour choisir l'analyse
    option = st.sidebar.selectbox(
        "Choisissez l'analyse à effectuer",
        [
            "Zones Fréquentées",
            "Tendances Temporelles",
            "Méthodes de Paiement",
            "Fournisseurs de Taxis",
        ],
    )

    if option == "Zones Fréquentées":
        # Analyse des zones les plus fréquentées
        st.header("📈 Zones les plus fréquentées")

        pickup_counts = df["zone_pickup"].value_counts().head(10)
        dropoff_counts = df["zone_dropoff"].value_counts().head(10)

        fig = make_subplots(rows=1, cols=2, shared_yaxes=True)

        # Graphique zones de prise en charge
        fig.add_trace(
            go.Bar(
                x=pickup_counts.index,
                y=pickup_counts.values,
                name="Zones de Prise en Charge",
            ),
            row=1,
            col=1,
        )
        # Graphique zones de dépôt
        fig.add_trace(
            go.Bar(
                x=dropoff_counts.index, y=dropoff_counts.values, name="Zones de Dépôt"
            ),
            row=1,
            col=2,
        )

        fig.update_layout(
            title="Top 10 des Zones de Prise en Charge et de Dépôt",
            height=500,
            showlegend=True,
            title_font=dict(size=24),
            margin=dict(t=80, b=40, l=40, r=40),
        )
        st.plotly_chart(fig)

    elif option == "Tendances Temporelles":
        # Analyse des tendances temporelles
        st.header("📈 Tendances Temporelles des Trajets")

        col1, col2 = st.columns(2)

        with col1:
            # Trajets par mois
            st.plotly_chart(
                create_bar_chart(
                    df_month,
                    "month",
                    "Nombre de trajets",
                    "Distribution des trajets par mois",
                    "Mois",
                    "Nombre de trajets",
                )
            )
            # Trajets par jour
            st.plotly_chart(
                create_bar_chart(
                    df_day,
                    "day",
                    "Nombre de trajets",
                    "Distribution des trajets par jour",
                    "Jour",
                    "Nombre de trajets",
                )
            )

        with col2:
            # Trajets par jour de la semaine
            st.plotly_chart(
                create_bar_chart(
                    df_week,
                    "week",
                    "Nombre de trajets",
                    "Distribution des trajets par jour de la semaine",
                    "Jour de la semaine",
                    "Nombre de trajets",
                )
            )
            # Trajets par heure
            st.plotly_chart(
                create_bar_chart(
                    df_hour,
                    "hour",
                    "Nombre de trajets",
                    "Distribution des trajets par heure",
                    "Heure",
                    "Nombre de trajets",
                )
            )

    elif option == "Méthodes de Paiement":
        # Analyse des méthodes de paiement
        st.header("📈 Répartition des Méthodes de Paiement")

        payment_counts = df["payment_method"].value_counts()
        fig = px.pie(
            payment_counts,
            names=payment_counts.index,
            values=payment_counts.values,
            title="Répartition des Méthodes de Paiement",
        )
        st.plotly_chart(fig)

    elif option == "Fournisseurs de Taxis":
        # Analyse des fournisseurs de taxis
        st.header("📈 Performance des Fournisseurs de Taxis")

        vendor_counts = df.groupby("vendor_name").size().reset_index(name="trajets")
        fig1 = create_bar_chart(
            vendor_counts,
            "vendor_name",
            "trajets",
            "Nombre de Trajets par Fournisseur",
            "Fournisseur",
            "Nombre de Trajets",
            color="trajets",  # Colorier les barres en fonction du trajet
        )

        vendor_amount = (
            df.groupby("vendor_name")["total_amount"]
            .mean()
            .reset_index(name="montant_moyen")
        )
        # Formater les valeurs de 'montant_moyen' avec 2 décimales
        vendor_amount["montant_moyen"] = vendor_amount["montant_moyen"].apply(
            lambda x: f"{x:.2f}"
        )
        fig2 = create_bar_chart(
            vendor_amount,
            "vendor_name",
            "montant_moyen",
            "Montant Moyen par Fournisseur",
            "Fournisseur",
            "Montant Moyen",
            color="montant_moyen",  # Colorier les barres en fonction du montant moyen
        )

        st.plotly_chart(fig1)
        st.plotly_chart(fig2)
