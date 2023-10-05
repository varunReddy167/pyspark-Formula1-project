-- Databricks notebook source
show databases

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use f1_processed;

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from drivers
limit 20;

-- COMMAND ----------

describe drivers;

-- COMMAND ----------

select * from drivers
WHERE nationality= 'British'
and dob >='1990-01-01'

-- COMMAND ----------

select  name, dob AS Date_of_birth from drivers
WHERE nationality= 'British'
and dob >='1990-01-01'
order by Date_of_birth  desc

-- COMMAND ----------


