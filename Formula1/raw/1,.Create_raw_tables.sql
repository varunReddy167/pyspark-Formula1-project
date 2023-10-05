-- Databricks notebook source
create  database  if not exists f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####create circuit table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1.raw.circuits(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "dbfs:/mnt/udemypyspark/raw/circuits.csv")
