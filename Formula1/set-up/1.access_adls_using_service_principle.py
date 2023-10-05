# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principal\
# MAGIC 1.Register Azure AD Application/ Service Principle\
# MAGIC 2.Generate a secrete/password for the application \
# MAGIC 3.set spark config with app/client id,directory.Tenantid & secret
# MAGIC 4.Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

Client_id=dls_account_key=dbutils.secrets.get(scope='Formula-project', key='client-id')
tenant_id=dbutils.secrets.get(scope='Formula-project', key='tenant-id')
Client_secret_value=dbutils.secrets.get(scope='Formula-project', key='Client-secret-value')

# COMMAND ----------



spark.conf.set("fs.azure.account.auth.type.udemypyspark.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.udemypyspark.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.udemypyspark.dfs.core.windows.net", Client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.udemypyspark.dfs.core.windows.net",Client_secret_value)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.udemypyspark.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@udemypyspark.dfs.core.windows.net"))


# COMMAND ----------

display(spark.read.csv("abfss://demo@udemypyspark.dfs.core.windows.net/circuits.csv"))
