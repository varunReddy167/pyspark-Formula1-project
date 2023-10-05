# Databricks notebook source
# MAGIC %md
# MAGIC ####Explore the capabilities of the dbutils.secrete utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='Formula-project')

# COMMAND ----------

dbutils.secrets.get(scope= 'Formula-project', key= 'client-id')

# COMMAND ----------


