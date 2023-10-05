# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake containers for project
# MAGIC
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    #get secret from key vault
    Client_id=dls_account_key=dbutils.secrets.get(scope='Formula-project', key='client-id')
    tenant_id=dbutils.secrets.get(scope='Formula-project', key='tenant-id')
    Client_secret_value=dbutils.secrets.get(scope='Formula-project', key='Client-secret-value')

    #set spark config
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": Client_id,
          "fs.azure.account.oauth2.client.secret": Client_secret_value,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    #unmountthe mount point if it already mounted    
    if any(mount.mountPoint==f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")


    #mount the storage account 
    dbutils.fs.mount(
         source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
         mount_point = f"/mnt/{storage_account_name}/{container_name}",
         extra_configs = configs)

    display(dbutils.fs.mounts())    


# COMMAND ----------

# MAGIC %md
# MAGIC ###Mount Raw container

# COMMAND ----------

mount_adls('udemypyspark','raw')

# COMMAND ----------

mount_adls('udemypyspark','processed')

# COMMAND ----------

mount_adls('udemypyspark','presentation')

# COMMAND ----------

dbutils.fs.ls("/mnt/udemypyspark/presentation")

