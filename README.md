##################################################################

# 🚕 Welcome to the Taxi-Tech Project

##################################################################

**Taxi-Tech** is a data analytics platform for New York's yellow taxi trips. It allows you to explore trip data to better understand demand, passenger behavior, and the performance of taxi providers.

## Features

-   **High-Demand Areas**  
    Identify the most popular locations for trips.

-   **Temporal Trends**  
    Analyze demand spikes by hour, day of the week, and special events.

-   **Payment Methods**  
    Explore passenger preferences for payment methods.

-   **Provider Performance**  
    Compare different taxi providers based on trip duration, wait times, and passenger satisfaction.

## Why Choose Taxi-Tech?

-   **Service Optimization**  
    Helps fleet managers improve fleet management and service responsiveness.

-   **Informed Decision Making**  
    Provides data for public authorities and transit managers to make informed decisions.

-   **Actionable Insights**  
    Interactive visualizations to improve system efficiency.

## Data Quality

To ensure the reliability and quality of the data used, **Taxi-Tech** integrates automated quality control mechanisms using **Soda**.

-   **Data Quality Control with Soda**  
    We use **Soda**, a data quality validation tool, to verify the integrity and consistency of the data before processing. Soda performs tests on various aspects of the data, such as missing values, data types, value ranges, and duplicates.

-   **Automation with Airflow**  
    The data validation process is automated with **Apache Airflow**. Airflow orchestrates data processing workflows, including quality tests, and ensures that the data is regularly checked and cleaned, guaranteeing high-quality analysis.

## Automated Data Download

To keep the data up to date, we have implemented an automated system to download the latest data every month.

-   **Data Download with MinIO**  
    The system automatically retrieves raw data every month from the **NYC Taxi & Limousine Commission** and stores it as **Parquet** files in **MinIO**. A Python script is used to download the data seamlessly, ensuring that new data is stored and ready for analysis.

-   **Automated Download with a Script**  
    A Python script fetches the data in CSV format, converts it to **Parquet** for better performance, and uploads it to **MinIO** storage. This process is fully automated and runs recurrently every month.

## Our Data

The data comes from the **NYC Taxi & Limousine Commission** and is stored in a **PostgreSQL** database, consisting of:

-   **Yellow Taxi Fact Table**: Information on taxi trips.
-   **Dimension Tables**: Geographical areas, time, payment methods, and taxi providers.

## Contact & Legal Information

-   **Email**: [contact@chagest.com](mailto:contact@chagest.com)
-   **Legal Information**: Public and anonymized data used for analysis purposes.

Version 1.0 | Copyright © 2025

##################################################################

## `DEPENDENCIES`

##################################################################

### In the project directory, run this command to initialize the project and install dependencies:

-   `python3 -m venv venv`
-   `source venv/bin/activate`
-   `python3 -m pip install --upgrade pip`
-   `pip install -r requirements.txt`
-   `pip freeze > requirements.txt`
-   `mkdir -p airflow/config airflow/dags airflow/logs airflow/plugins data docs models notebooks references reports src/data src/features src/models src/visualization`
-   `brew install --cask docker`
-   `brew install kubectl`
-   `brew install docker-compose`
-   `docker compose up -d`
-   `psql -h localhost -p PORT -U USERNAME DATABASE`

### Command to visualized the data:

-   `pip install streamlit streamlit-option-menu matplotlib geopandas seaborn plotly python-dotenv`
-   `pip freeze > requirements.txt`
-   `streamlit run app.py`

### Environment variables (inside the file .env):

-   `MINIO_HOSTNAME=minio`
-   `MINIO_PORT=9000`
-   `MINIO_ACCESS_KEY=minio`
-   `MINIO_SECRET_KEY=minio`
-   `WH_DBMS_USERNAME=postgres`
-   `WH_DBMS_PASSWORD=admin`
-   `WH_DBMS_IP=localhost`
-   `WH_DBMS_PORT=15432`
-   `WH_DBMS_DATABASE=tp_warehouse`
-   `WH_DBMS_TABLE=warehouse`
-   `DM_DBMS_USERNAME=postgres`
-   `DM_DBMS_PASSWORD=admin`
-   `DM_DBMS_IP=localhost`
-   `DM_DBMS_PORT=15434`
-   `DM_DBMS_DATABASE=tp_datamart`
-   `WH_DBLINK_IP=db-warehouse`
-   `WH_DBLINK_PORT=5432`
-   `WH_DBLINK_DATABASE=tp_warehouse`
-   `WH_DBLINK_USERNAME=postgres`
-   `WH_DBLINK_PASSWORD=admin`
-   `DOCKER_DB_USERNAME=admin`
-   `DOCKER_DB_PASSWORD=admin`
-   `DOCKER_AIRFLOW_USERNAME=airflow`
-   `DOCKER_AIRFLOW_PASSWORD=airflow`
-   `DOCKER_AIRFLOW_DB=airflow`
-   `DOCKER_AIRFLOW_PORT=15433`
-   `DOCKER_DBMS_USERNAME=postgres`
-   `DOCKER_DBMS_PASSWORD=admin`
-   `DOCKER_DBMS_IP=db-datamart`
-   `DOCKER_DBMS_PORT=5432`
-   `DOCKER_DBMS_DATABASE=tp_datamart`
-   `DOCKER_DBMS_DATASOURCE=tp_datamart`
