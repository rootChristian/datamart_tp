-- ***********************************************************************
-- ************** Author:   Christian KEMGANG NGUESSOP *******************
-- ************** Project:   datamart                  *******************
-- ************** Version:  1.0.0                      *******************
--***********************************************************************

--SELECT * FROM pg_extension WHERE extname = 'dblink';

--CREATE EXTENSION dblink;


------------------------------------------------------
-----------------DIMENSION ZONE-----------------------
------------------------------------------------------
-- Insertion des valeurs distinctes dans la dimension zone: depuis la fonction insert_data_from_csv() dans le code datawarehouse_to_datamart_olap.py


------------------------------------------------------
-----------------DIMENSION PAYMENT--------------------
------------------------------------------------------
-- Insertion des valeurs distinctes dans la dimension paiement
INSERT INTO public.dimension_payment  (id_payment_type, payment_method)
values
	(0, 'Voided trip'),
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown');


------------------------------------------------------
-----------------DIMENSION VENDOR---------------------
------------------------------------------------------
-- Insertion des valeurs distinctes dans la dimension vendor avec une valeur par défaut pour le `vendor_name`
INSERT INTO public.dimension_vendor (id_vendor, vendor_name)
SELECT
    wh.vendorid AS id_vendor,  -- L'ID du vendeur, tel qu'il existe dans la source
    CASE
        WHEN wh.vendorid = 1 THEN 'Creative Mobile Technologies, LLC'  -- Valeur spécifique pour vendorid = 1
        WHEN wh.vendorid = 2 THEN 'VeriFone Inc.'  -- Valeur spécifique pour vendorid = 2
        ELSE COALESCE('Unknown Vendor')  -- Valeur par défaut pour les autres cas
    END AS vendor_name
-- Récupérer les `vendorid` distincts depuis la source
FROM dblink(
    'host=XXXXX port=5432 dbname=XXXXX user=XXXXX password=XXXXX',
    'SELECT DISTINCT vendorid FROM warehouse'
) AS wh(vendorid INT)
-- Condition pour éviter les doublons dans la dimension vendor
WHERE NOT EXISTS (
    SELECT 1 
    FROM public.dimension_vendor 
    WHERE dimension_vendor.id_vendor = wh.vendorid
);


------------------------------------------------------
-----------------DIMENSION TIME-----------------------
------------------------------------------------------
-- Insertion des valeurs distinctes dans la dimension temps
INSERT INTO public.dimension_time (id_time, year, month, day, hour, minute, seconde, week, trimester)
SELECT
    EXTRACT(EPOCH FROM tpep_datetime)::INTEGER AS id_time,  -- Conversion de la date en timestamp UNIX
    EXTRACT(YEAR FROM tpep_datetime)::INTEGER AS year,
    EXTRACT(MONTH FROM tpep_datetime)::INTEGER AS month,
    EXTRACT(DAY FROM tpep_datetime)::INTEGER AS day,
    EXTRACT(HOUR FROM tpep_datetime)::INTEGER AS hour,
    EXTRACT(MINUTE FROM tpep_datetime)::INTEGER AS minute,
    EXTRACT(SECOND FROM tpep_datetime)::INTEGER AS seconde,
    EXTRACT(WEEK FROM tpep_datetime)::INTEGER AS week,
    EXTRACT(QUARTER FROM tpep_datetime)::INTEGER AS trimester
FROM (
    -- Sélection des dates pickup et dropoff dans une seule requête dblink
    SELECT tpep_pickup_datetime AS tpep_datetime
    FROM dblink(
        'host=XXXXX port=5432 dbname=XXXXX user=XXXXX password=XXXXX',
        'SELECT DISTINCT tpep_pickup_datetime FROM warehouse'
    ) AS wh(
        tpep_pickup_datetime TIMESTAMP
    )
    UNION
    -- Union des dates pickup et dropoff en une seule colonne tpep_datetime
    SELECT tpep_dropoff_datetime AS tpep_datetime
    FROM dblink(
        'host=XXXXX port=5432 dbname=XXXXX user=XXXXX password=XXXXX',
        'SELECT DISTINCT tpep_dropoff_datetime FROM warehouse'
    ) AS wh(
        tpep_dropoff_datetime TIMESTAMP
    )
) AS combined_dates
-- Condition pour éviter les doublons dans la dimension temps
WHERE NOT EXISTS (
    SELECT 1 
    FROM public.dimension_time 
    WHERE dimension_time.id_time = EXTRACT(EPOCH FROM combined_dates.tpep_datetime)::INTEGER
);


------------------------------------------------------
-----------------DIMENSION FACTURE--------------------
------------------------------------------------------
-- Insertion des valeurs distinctes dans le fait facture
INSERT INTO public.fact_yellow_taxi(id_vendor, id_time_pickup, id_time_dropoff, id_zone_pickup, id_zone_dropoff, id_payment_type, fare_amount,
                                    extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee)
SELECT
    vendorid as id_vendor,
    EXTRACT(EPOCH FROM wh.tpep_pickup_datetime)::INTEGER AS id_time_pickup,
    EXTRACT(EPOCH FROM wh.tpep_dropoff_datetime)::INTEGER AS id_time_dropoff,
    pulocationid AS id_zone_pickup,
    dolocationid AS id_zone_dropoff,
    payment_type AS id_payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee
FROM dblink(
    'host=XXXXX port=5432 dbname=XXXXX user=XXXXX password=XXXXX',
    'SELECT vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, pulocationid, dolocationid, 
            payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, 
            improvement_surcharge, total_amount, congestion_surcharge, airport_fee
     FROM warehouse'
) AS wh(
    vendorid INT,  -- vendorid est de type INT4
    tpep_pickup_datetime TIMESTAMP, 
    tpep_dropoff_datetime TIMESTAMP, 
    pulocationid INT, 
    dolocationid INT, 
    payment_type INT8,  -- payment_type est de type INT8 (BIGINT)
    fare_amount DECIMAL, 
    extra DECIMAL, 
    mta_tax DECIMAL, 
    tip_amount DECIMAL, 
    tolls_amount DECIMAL, 
    improvement_surcharge DECIMAL, 
    total_amount DECIMAL, 
    congestion_surcharge DECIMAL, 
    airport_fee DECIMAL
)
