-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Drop all the tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed  CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
Location "/mnt/udemypyspark/processed";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation  CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
Location "/mnt/udemypyspark/presentation";
