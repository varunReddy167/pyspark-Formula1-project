# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys\
# MAGIC 1.set the spark config fs.azure.account.key\
# MAGIC 2.List files from demo cointainer \
# MAGIC 3.Read data from circuit.csv

# COMMAND ----------

dls_account_key=dbutils.secrets.get(scope='Formula-project', key='Access-Key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.udemypyspark.dfs.core.windows.net",
    dls_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@udemypyspark.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@udemypyspark.dfs.core.windows.net/circuits.csv"))
