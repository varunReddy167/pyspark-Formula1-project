# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token\
# MAGIC 1.set the spark config for SAS Token\
# MAGIC 2.List files from demo cointainer \
# MAGIC 3.Read data from circuit.csv

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.udemypyspark.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.udemypyspark.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.udemypyspark.dfs.core.windows.net", dbutils.secrets.get(scope="Formula-project", key='SAS-Token'))

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@udemypyspark.dfs.core.windows.net"))
#databricks-scope

# COMMAND ----------

display(spark.read.csv("abfss://demo@udemypyspark.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

dbutils.secrets.get
