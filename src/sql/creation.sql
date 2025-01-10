-- ***********************************************************************
-- ************** Author:   Christian KEMGANG NGUESSOP *******************
-- ************** Project:   datamart                  *******************
-- ************** Version:  1.0.0                      *******************
-- ***********************************************************************

-- Table Dimension Vendor (Compagnie de taxi)
CREATE TABLE IF NOT EXISTS dimension_vendor (
    --id_vendor SERIAL PRIMARY KEY,   -- Identifiant unique pour la dimension de vendeur
    id_vendor INT UNIQUE,               -- Identifiant du vendeur venant de la table source `warehouse`
    vendor_name VARCHAR(255)
);

-- Table Dimension Temps (Date, mois, année, semaine)
CREATE TABLE IF NOT EXISTS dimension_time (
    --id_time SERIAL PRIMARY KEY,  -- Identifiant de la dimension du temps
    id_time INT UNIQUE,          -- Identifiant du temps (par exemple, l'epoch en secondes)
    year INT,
    month INT,
    day INT,
    hour INT,
    minute INT,
    seconde INT,
    week INT,
    trimester INT
);

-- Table Dimension Zone (Zone de prise en charge et de dépose)
CREATE TABLE IF NOT EXISTS dimension_zone (
    --id_zone SERIAL PRIMARY KEY,  -- Identifiant unique pour la zone
    id_zone INT UNIQUE,              -- Identifiant de la zone (par exemple, `pulocationid`, `dolocationid`)
    borough VARCHAR(255),
    name_zone VARCHAR(255),
    service_zone VARCHAR(255)
);

-- Table Dimension Paiement (Type de paiement)
CREATE TABLE IF NOT EXISTS dimension_payment (
    --id_payment SERIAL PRIMARY KEY,     -- Identifiant unique pour le type de paiement
    id_payment_type INT UNIQUE,           -- Identifiant du type de paiement (par exemple, `payment_type` de `warehouse`)
    payment_method VARCHAR(255)        -- Description du type de paiement (par exemple, "Carte de crédit", "Espèces")
);

-- Table des faits - fact_yellow_taxi
CREATE TABLE IF NOT EXISTS fact_yellow_taxi (
    --id_fact_yellow_taxi SERIAL PRIMARY KEY,                  -- Identifiant unique de la transaction
    id_vendor INT NOT NULL REFERENCES dimension_vendor(id_vendor) 
        ON DELETE CASCADE,                           -- Clé étrangère vers la table Vendor (supprimer toutes les courses si un vendeur est supprimé)
    id_time_pickup INT NOT NULL REFERENCES dimension_time(id_time) 
        ON DELETE CASCADE,                           -- Clé étrangère vers la table Temps (supprimer les courses si le temps de pickup est supprimé)
    id_time_dropoff INT NOT NULL REFERENCES dimension_time(id_time) 
        ON DELETE CASCADE,                           -- Clé étrangère vers la table Temps (supprimer les courses si le temps de dropoff est supprimé)
    id_zone_pickup INT NOT NULL REFERENCES dimension_zone(id_zone) 
        ON DELETE CASCADE,                           -- Clé étrangère vers la table Zone pour la prise en charge (supprimer les courses si la zone de pickup est supprimée)
    id_zone_dropoff INT NOT NULL REFERENCES dimension_zone(id_zone) 
        ON DELETE CASCADE,                           -- Clé étrangère vers la table Zone pour la dépose (supprimer les courses si la zone de dropoff est supprimée)
    id_payment_type INT NOT NULL REFERENCES dimension_payment(id_payment_type) 
        ON DELETE SET NULL,                          -- Clé étrangère vers la table Paiement (si le type de paiement est supprimé, laisser les courses avec un paiement NULL)
    fare_amount DECIMAL(10, 2), -- Montant du tarif de base
    extra DECIMAL(10, 2),   -- Montant des frais supplémentaires
    mta_tax DECIMAL(10, 2), -- Montant de la taxe MTA
    tip_amount DECIMAL(10, 2),  -- Montant du pourboire
    tolls_amount DECIMAL(10, 2),    -- Montant des péages
    improvement_surcharge DECIMAL(10, 2),   -- Frais de surcharge
    total_amount DECIMAL(10, 2),    -- Montant total de la course
    congestion_surcharge DECIMAL(10, 2),    -- Surcharge de congestion
    airport_fee DECIMAL(10, 2)  -- Frais aéroport
);

-- Création d'index sur les clés étrangères pour améliorer les performances des jointures
CREATE INDEX IF NOT EXISTS idx_fact_vendor ON fact_yellow_taxi(id_vendor);
CREATE INDEX IF NOT EXISTS idx_fact_time_pickup ON fact_yellow_taxi(id_time_pickup);
CREATE INDEX IF NOT EXISTS idx_fact_time_dropoff ON fact_yellow_taxi(id_time_dropoff);
CREATE INDEX IF NOT EXISTS idx_fact_zone_pickup ON fact_yellow_taxi(id_zone_pickup);
CREATE INDEX IF NOT EXISTS idx_fact_zone_dropoff ON fact_yellow_taxi(id_zone_dropoff);
CREATE INDEX IF NOT EXISTS idx_fact_payment_type ON fact_yellow_taxi(id_payment_type);
