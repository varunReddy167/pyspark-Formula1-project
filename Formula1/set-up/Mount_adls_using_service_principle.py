# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake using Service Principal
# MAGIC ####steps to follow
# MAGIC 1.Get clentid.tenantid and clent_secret from key vault\
# MAGIC 2.set spark config with APp/clentid.tenantid and clent_secret\
# MAGIC 3.Call file system utility mount to mount the storage\
# MAGIC 4.Explore others file sysytem utilities to mount(list all mounts,unmount)

# COMMAND ----------

Client_id=dls_account_key=dbutils.secrets.get(scope='Formula-project', key='client-id')
tenant_id=dbutils.secrets.get(scope='Formula-project', key='tenant-id')
Client_secret_value=dbutils.secrets.get(scope='Formula-project', key='Client-secret-value')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": Client_id,
          "fs.azure.account.oauth2.client.secret": Client_secret_value,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@udemypyspark.dfs.core.windows.net/",
  mount_point = "/mnt/udemypyspark/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls('/mnt/udemypyspark/demo'))


# COMMAND ----------

display(spark.read.csv("/mnt/udemypyspark/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/udemypyspark/demo')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls
# MAGIC /mnt/udemypyspark/raw
