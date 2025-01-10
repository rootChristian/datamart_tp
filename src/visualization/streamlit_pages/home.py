"""
***********************************************************************
************** Author:   Christian KEMGANG NGUESSOP *******************
************** Project:   datamart                  *******************
************** Version:  1.0.0                      *******************
***********************************************************************
"""

import streamlit as st


# Fonction pour afficher la page d'accueil du projet Yellow Taxi
def show_home():
    # Titre principal de l'application
    st.title("🚕 Bienvenue sur Taxi-Tech")

    st.markdown(
        """
        <p style="text-align: left; padding-left: 5%; font-size: 20px; color: #7F8C8D; font-weight: bold; /*text-transform: uppercase*/">
            Explorez les trajets des taxis jaunes de New York.
        </p>
    """,
        unsafe_allow_html=True,
    )

    # Description générale du projet
    st.markdown(
        """
        **Taxi-Tech** est une plateforme innovante d'analyse de données qui vous permet d'explorer les trajets effectués par les célèbres taxis jaunes de New York.  
        Grâce à l'exploitation de grands ensembles de données, notre application fournit des insights détaillés sur les tendances, les zones de forte demande, les comportements des passagers et les performances des fournisseurs de taxis.
        
        ### Notre mission
        Offrir aux analystes, aux opérateurs de taxis et aux autorités publiques des outils puissants pour optimiser la gestion du transport urbain, améliorer l'efficacité des services et enrichir l'expérience des passagers à New York.
    """
    )

    # Fonctionnalités clés
    st.header("🧭 Fonctionnalités clés")

    # Liste des fonctionnalités
    st.markdown(
        """
        1. **Analyse des zones à forte demande**  
            Découvrez les endroits les plus fréquentés par les passagers. Grâce à des cartes interactives et des visualisations, vous pourrez identifier les zones géographiques avec la plus grande concentration de trajets. Ces informations permettent de mieux comprendre la dynamique de la demande dans la ville.

        2. **Exploration des tendances temporelles**  
            Visualisez les tendances de la demande de taxis à différentes heures de la journée, jours de la semaine, et pendant les événements spéciaux. Cette analyse permet d'anticiper les pics de demande et d'optimiser l'affectation des taxis.

        3. **Répartition des méthodes de paiement**  
            Obtenez un aperçu des méthodes de paiement utilisées par les passagers. Explorez les préférences en matière de paiements électroniques, en espèces, ou via des applications mobiles. Ces informations sont cruciales pour comprendre l’adoption de nouvelles technologies de paiement dans le secteur du transport.

        4. **Comparaison des performances des fournisseurs de taxis**  
            Évaluez et comparez la performance des différents fournisseurs de taxis sur la base de critères tels que la durée des trajets, le temps d'attente, et la satisfaction des passagers. Ces comparaisons aident à mesurer l'efficacité des services et à identifier les opportunités d'amélioration.
    """
    )

    # Pourquoi Taxi-Tech
    st.header("Pourquoi Taxi-Tech ?")
    st.markdown(
        """
        - **Optimisation du service** : En fournissant des données précises sur les zones à forte demande et les périodes de pointe, nous aidons les opérateurs à mieux gérer leur flotte et à améliorer la réactivité du service.
        
        - **Meilleure prise de décision** : Nos analyses permettent aux autorités publiques et aux gestionnaires de prendre des décisions éclairées sur l'aménagement urbain, les politiques de transport et la gestion du trafic.
        
        - **Insights actionnables** : Grâce à des visualisations claires et interactives, les utilisateurs peuvent facilement identifier des tendances clés et agir en conséquence pour améliorer l'efficacité du système de transport.
    """
    )

    # Données utilisées
    st.header("📊 Nos Données")
    st.markdown(
        """
        Les données utilisées par **Taxi-Tech** proviennent de la NYC Taxi & Limousine Commission (Commission des Taxis et Limousines de New York), qui fournit des enregistrements détaillés sur chaque trajet effectué par les taxis jaunes de New York. Ces données ont été traitées avant d’être insérées dans une base de données PostgreSQL comprenant les tables suivantes:
        
        - **Table des faits des taxis jaunes** : Contient les informations sur les trajets effectués par les taxis.
        
        - **Tables dimensionnelles (zone, temps, paiements et fournisseurs)** : Contiennent des informations dimensionnelles associées aux trajets.
    """
    )

    # Footer avec Mentions Légales et Contact
    st.header("📞 Contact & Mentions Légales")
    st.markdown(
        """
        - Pour plus d’informations, vous pouvez nous contacter à l’adresse suivante : [contact@chagest.com](mailto:contact@chagest.com).
        - **Mentions légales** : L'application utilise des données publiques et anonymisées pour des fins d'analyse et d'optimisation des trajets de taxis.
        
        Version 1.0 | Copyright © 2025
    """
    )
