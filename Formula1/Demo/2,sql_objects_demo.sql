-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Lesson objectives
-- MAGIC 1.Spark SQL documentation\
-- MAGIC 2.create database demo\
-- MAGIC 3.data tabin the ui\
-- MAGIC 4.SHOW Command\
-- MAGIC 5.DESCRIBE Command\
-- MAGIC 6.Find the current database

-- COMMAND ----------

CREATE DATABASE  IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED default;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in default;

-- COMMAND ----------

use default;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables in default;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Learning objectives
-- MAGIC 1.Create managed tables using python\
-- MAGIC 2.Create managed table using sql\
-- MAGIC 3.Effect of dropping a manged table\
-- MAGIC 4.Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

show tables in demo; 

-- COMMAND ----------

USE demo;

-- COMMAND ----------


DESC EXTENDED  race_results_python

-- COMMAND ----------

SELECT * 
FROM  demo.race_results_python
WHERE race_year = 2020

-- COMMAND ----------

use demo

-- COMMAND ----------

CREATE TABLE  IF NOT EXISTS demo.race_results_sql
AS
SELECT * 
FROM  demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

DESC  EXTENDED demo.race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Learning objectives
-- MAGIC 1.Create external table using Python\
-- MAGIC 2.Create external table using sq\
-- MAGIC 3.Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC  EXTENDED demo.race_results_ext_py

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

DROP  TABLE race_results_ext_py;

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Views on tables
-- MAGIC ####Learning objectives
-- MAGIC 1.Create Temp View\
-- MAGIC 2.Create Global Temp View\
-- MAGIC 3.Create Permanent View
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMP  VIEW v_race_results
As 
SELECT * FROM  
demo.race_results_python
WHERE race_year=2018

-- COMMAND ----------

SELECT * FROM  v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP  VIEW gv_race_results
As 
SELECT * FROM  
demo.race_results_python
WHERE race_year=2013

-- COMMAND ----------

SELECT * FROM  global_temp.gv_race_results

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

CREATE OR REPLACE  VIEW pv_race_results
As 
SELECT * FROM  
demo.race_results_python
WHERE race_year=2012

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from pv_race_results
