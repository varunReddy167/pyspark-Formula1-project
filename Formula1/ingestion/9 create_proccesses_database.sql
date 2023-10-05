-- Databricks notebook source
create database if not exists f1_processed
LOCATION "/mnt/udemypyspark/processed"

-- COMMAND ----------

describe database f1_processed
