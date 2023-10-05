# Databricks notebook source
# MAGIC %md
# MAGIC ###Explore DBFS Root
# MAGIC 1.List All the folders in DBFS Root\
# MAGIC 2.intract with dbfs file browser\
# MAGIC 3.upload file to Dbfs root

# COMMAND ----------

display(dbutils.fs.ls('/'))
