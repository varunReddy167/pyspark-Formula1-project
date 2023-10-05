-- Databricks notebook source
use f1_processed

-- COMMAND ----------


SELECT *, CONCAT(driver_ref, '-', code) AS new_driver_ref
FROM drivers;

-- COMMAND ----------

SELECT *, split(name,' ')[0] forename,split(name,' ')[1] surename
FROM drivers;

-- COMMAND ----------

SELECT *, current_timestamp()
FROM drivers;

-- COMMAND ----------

SELECT *, date_format(dob,'dd-MM-yyyy')
FROM drivers;

-- COMMAND ----------

SELECT nationality,name,dob,rank() OVER (PARTITION BY nationality ORDER BY dob DESC) AS age_rank
FROM drivers
ORDER BY nationality,age_rank
