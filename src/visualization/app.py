"""
***********************************************************************
************** Author:   Christian KEMGANG NGUESSOP *******************
************** Project:   datamart                  *******************
************** Version:  1.0.0                      *******************
***********************************************************************
"""

import streamlit as st
from streamlit_option_menu import option_menu  # Importation de option_menu
import streamlit_pages.home as home
import streamlit_pages.data as data
import streamlit_pages.dashboard as dashboard

# Titre dans la barre latérale avec Flexbox pour aligner l'image et le titre
st.sidebar.markdown(
    """
    <style>
        .sidebar-container {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            margin-bottom: 15px; /* Ajouter un espace sous le logo */
        }

        .sidebar-logo {
            border-radius: 50%;
            height: 40%; /* Ajustez la taille de l'image */
            width: 40%;
            margin-bottom: 10px;  /* Ajouter un espace sous l'image */
        }

        .sidebar-title {
            text-align: center;
            text-shadow: 1px 1px 0 #bcbcbc, 2px 2px 0 #9c9c9c;
            color: #CCCCCC;
            font-size: 40px;  /* Ajuster la taille du titre */
        }

        /* Style du menu de la barre latérale */
        .css-1kyxreq { 
            background-color: #2a2a2a;
            color: white;
        }
    </style>
    
    <div class="sidebar-container">
        <img src="https://firebasestorage.googleapis.com/v0/b/chagest-eshop.appspot.com/o/Logo%2Flogo.png?alt=media&token=9b7a9042-f7d7-46c7-b321-bc5cd40cd335" 
             class="sidebar-logo" />
        <h1 class="sidebar-title">TAXI-TECH</h1>
    </div>
    """,
    unsafe_allow_html=True,
)


# Fonction pour la barre latérale
def sideBar():
    # Menu latéral avec les options
    with st.sidebar:
        selected = option_menu(
            menu_title="Menu Principal",
            options=["Home", "Data", "Dashboard"],
            icons=["house", "database", "graph-up"],
            menu_icon="cast",
            default_index=0,
            orientation="vertical",
            styles={"container": {"padding": "20px"}},  # Option de style pour le menu
        )
        return selected


# Fonction pour afficher le contenu en fonction de la page sélectionnée
def render_content(selected_page):
    if selected_page == "Home":
        home.show_home()  # Appel de la fonction pour afficher la page d'accueil
    elif selected_page == "Data":
        data.show_data()  # Appel de la fonction pour afficher les données
    elif selected_page == "Dashboard":
        dashboard.show_dashboard()  # Appel de la fonction pour afficher le dashboard
    else:
        st.title("Page Introuvable")
        st.write("Désolé, cette page n'existe pas.")


# Fonction principale pour l'application
def main():
    # Obtenir l'option sélectionnée dans la barre latérale
    selected = sideBar()

    # Afficher la page selon la sélection
    render_content(selected)


# Exécution de l'application
if __name__ == "__main__":
    main()
