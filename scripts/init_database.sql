/*
=============================================================
Data Warehouse Layers in MySQL
=============================================================
Script Purpose:
    Creates the DataWarehouse database with Bronze, Silver, and 
    Gold placeholder tables. Drops the database first if it exists 
    to ensure a clean environment.
*/

DROP DATABASE IF EXISTS DataWarehouse;
CREATE DATABASE DataWarehouse;
USE DataWarehouse;

-- Bronze layer (raw data ingestion)
CREATE TABLE bronze_tablename (
    id INT PRIMARY KEY
);

-- Silver layer (cleaned/validated data)
CREATE TABLE silver_tablename (
    id INT PRIMARY KEY
);

-- Gold layer (curated/aggregated data for reporting)
CREATE TABLE gold_tablename (
    id INT PRIMARY KEY
);
