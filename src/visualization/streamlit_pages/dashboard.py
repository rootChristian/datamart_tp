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
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dotenv import load_dotenv

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Config datamart
dm_dbms_username = os.getenv("DM_DBMS_USERNAME")
dm_dbms_password = os.getenv("DM_DBMS_PASSWORD")
dm_dbms_ip = os.getenv("DM_DBMS_IP")
dm_dbms_port = os.getenv("DM_DBMS_PORT")
dm_dbms_database = os.getenv("DM_DBMS_DATABASE")


# Fonction pour se connecter à PostgreSQL avec psycopg2
def connect_to_db():
    try:
        # Connexion à PostgreSQL avec psycopg2
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


# Charger les données depuis PostgreSQL avec un cache pour éviter de charger tout le temps
@st.cache_data(ttl=86400)  # Cache pendant 24 heures (modifiable)
def load_data():
    conn = connect_to_db()  # Connexion à la base de données avec psycopg2
    if conn is None:
        return pd.DataFrame()  # Retourne un DataFrame vide si la connexion échoue

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
        LIMIT 1000000
    """
    try:
        # Exécuter la requête et retourner les résultats sous forme de DataFrame
        df = pd.read_sql(query_fact, conn)
        return df
    except Exception as e:
        st.error(f"Erreur lors du chargement des données : {e}")
        return pd.DataFrame()  # Retourne un DataFrame vide en cas d'erreur
    finally:
        conn.close()  # Fermer la connexion à la base de données


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
def create_bar_chart(
    data, x_col, y_col, title, x_title, y_title, y_init_value, color=None
):
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

    # Après avoir créé le graphique dans create_bar_chart, vous pouvez ajuster l'axe Y
    fig.update_yaxes(range=[y_init_value, max(data[y_col]) + y_init_value])

    # Mise à jour de la mise en page
    fig.update_layout(
        title_font=dict(family="Arial", size=24, color="black"),
        template="none",  # Choisir le style de la mise en page
        showlegend=True,  # Pas besoin de légende ici
        margin=dict(
            l=80, r=40, t=40, b=90
        ),  # Ajuste les marges pour un meilleur espacement
    )

    return fig


# Fonction de création de graphiques
def create_bar_chart_bis(
    data,
    x_col,
    y_col,
    title,
    x_title,
    y_title,
    valeur_initiale_max,
    color=None,
    threshold=200000,
):
    """
    Crée un graphique à barres verticales avec Plotly.
    - Supprime les valeurs insignifiantes (en dessous du seuil).
    """
    # Filtrer les données pour exclure les valeurs insignifiantes
    data_filtered = data[data[y_col] >= threshold]

    # Si color est passé, on l'utilise pour colorier les barres, sinon on les colore d'une couleur fixe
    if color:
        fig = px.bar(
            data_filtered,
            x=x_col,
            y=y_col,
            labels={x_col: x_title, y_col: y_title},
            title=title,
            color=color,  # Utilisation de la colonne spécifiée pour la couleur
            color_continuous_scale="Viridis",  # Choix du dégradé de couleurs
        )
    else:
        fig = px.bar(
            data_filtered,
            x=x_col,
            y=y_col,
            labels={x_col: x_title, y_col: y_title},
            title=title,
            # color_discrete_sequence=["skyblue"],  # Choisit une couleur fixe si color n'est pas passé
        )

    # Ajuster l'échelle de l'axe Y (utiliser "montant_moyen" pour calculer la plage)
    fig.update_yaxes(range=[200000, valeur_initiale_max + 100000])

    # Mise à jour de la mise en page
    fig.update_layout(
        title_font=dict(family="Arial", size=24, color="black"),
        template="none",  # Choisir le style de la mise en page
        showlegend=True,  # Pas besoin de légende ici
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

        # Ajuster l'axe y pour commencer à une certaine valeur
        fig.update_yaxes(
            range=[20000, max(pickup_counts.values).max() + 20000], row=1, col=1
        )
        fig.update_yaxes(
            range=[20000, max(dropoff_counts.values).max() + 20000], row=1, col=2
        )

        fig.update_layout(
            title="Top 10 des Zones de Prise en Charge et de Dépôt",
            height=500,
            showlegend=True,
            title_font=dict(size=24),
            margin=dict(t=80, b=40, l=40, r=40),
            xaxis_title="Nom des zones",  # Titre de l'axe X pour les deux graphiques
            yaxis_title="Nombre de trajets",  # Titre de l'axe Y pour les deux graphiques
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
                    100000,  # Valeur initiale sur l'axe des Y
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
                    15000,  # Valeur initiale sur l'axe des Y
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
                    20000,  # Valeur initiale sur l'axe des Y
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
                    3000,  # Valeur initiale sur l'axe des Y
                )
            )

    elif option == "Méthodes de Paiement":
        # Analyse des méthodes de paiement
        st.header("📈 Répartition des Méthodes de Paiement")

        # Calcul des valeurs uniques et leurs fréquences pour la colonne "payment_method"
        payment_counts = df["payment_method"].value_counts()

        # Création de l'histogramme vertical (en mettant les méthodes de paiement sur l'axe vertical)
        fig = px.bar(
            payment_counts,
            y=payment_counts.index,  # Les méthodes de paiement sur l'axe Y
            x=payment_counts.values,  # Fréquences de chaque méthode sur l'axe X
            labels={  # Labels des axes
                "y": "Méthode de Paiement",  # Label de l'axe Y
                "x": "Nombre de Transactions",  # Label de l'axe X
            },
            color=payment_counts.index,  # Colorier les barres par méthode de paiement
            # color_discrete_sequence=px.colors.qualitative.Set1,  # Palette de couleurs
        )

        # Ajout des légendes et autres options de style
        fig.update_layout(
            xaxis_title="Nombre de Transactions",  # Titre de l'axe X
            yaxis_title="Méthode de Paiement",  # Titre de l'axe Y
            showlegend=True,  # Afficher la légende
            legend_title="Méthodes de Paiement",  # Titre de la légende
            legend_orientation="v",  # Orientation horizontale de la légende
            # legend=dict(x=1, y=1),  # Position de la légende en dessous du graphique
        )

        # Affichage du graphique dans Streamlit
        st.plotly_chart(fig)
        """
        payment_counts = df["payment_method"].value_counts()
        fig = px.pie(
            payment_counts,
            names=payment_counts.index,
            values=payment_counts.values,
            title="Répartition des Méthodes de Paiement",
        )
        st.plotly_chart(fig)
        """
    elif option == "Fournisseurs de Taxis":
        # Analyse des fournisseurs de taxis
        st.header("📈 Performance des Fournisseurs de Taxis")

        vendor_counts = df.groupby("vendor_name").size().reset_index(name="trajets")

        # Extraire la valeur maximale de "trajets" pour définir la plage de l'axe Y
        valeur_initiale_max = vendor_counts["trajets"].max()

        fig1 = create_bar_chart_bis(
            vendor_counts,
            "vendor_name",
            "trajets",
            "Nombre de Trajets par Fournisseur",
            "Nom des fournisseurs",
            "Nombre de Trajets",
            valeur_initiale_max,
            color="trajets",  # Colorier les barres en fonction du trajet
        )

        # Calcul du montant moyen par fournisseur
        vendor_amount = (
            df.groupby("vendor_name")["total_amount"]
            .mean()
            .reset_index(name="montant_moyen")
        )

        # Formater les valeurs de 'montant_moyen' avec 2 décimales
        vendor_amount["montant_moyen"] = vendor_amount["montant_moyen"].apply(
            lambda x: f"{x:.2f}"
        )

        # Ajouter la colonne 'montant_moyen' comme texte pour les labels des barres
        vendor_amount["label"] = (
            vendor_amount["vendor_name"] + ": " + vendor_amount["montant_moyen"]
        )

        # Création de l'histogramme vertical (en mettant les fournisseurs sur l'axe X et montant_moyen sur l'axe Y)
        fig2 = px.bar(
            vendor_amount,
            x="vendor_name",  # Les fournisseurs sur l'axe X
            y="montant_moyen",  # Montant moyen sur l'axe Y
            title="Montant Moyen par Fournisseur",
            color="montant_moyen",  # Colorier les barres par montant moyen
            # text="label",  # Afficher les labels avec le montant moyen sur chaque barre
            color_continuous_scale="Viridis",  # px.colors.sequential.Blues,  # Palette de couleurs (dégradé bleu)
        )

        # Convertir la colonne 'montant_moyen' en valeurs numériques (en cas de valeurs non numériques, elles seront converties en NaN)
        vendor_amount["montant_moyen"] = pd.to_numeric(
            vendor_amount["montant_moyen"], errors="coerce"
        )

        # Calculer la valeur maximale après conversion en numérique
        valeur_max = vendor_amount["montant_moyen"].max()

        # Ajuster l'échelle de l'axe Y (utiliser "montant_moyen" pour calculer la plage)
        fig2.update_yaxes(range=[20, valeur_max + 5])

        # Ajout des légendes et autres options de style
        fig2.update_layout(
            xaxis_title="Nom des fournisseurs",  # Titre de l'axe X
            yaxis_title="Montant moyen",  # Titre de l'axe Y
            showlegend=True,  # Afficher la légende
            legend_title="Montant Moyen",  # Titre de la légende
            legend_orientation="v",  # Orientation verticale de la légende
            title_font=dict(
                family="Arial", size=24, color="black"
            ),  # Taille de la police du titre
            title_x=0.5,  # Centrer le titre horizontalement
            title_xanchor="center",  # Centrer par rapport à l'axe X
            margin=dict(t=40, b=40, l=40, r=40),  # Marges autour du graphique
        )

        st.plotly_chart(fig1)
        st.plotly_chart(fig2)
