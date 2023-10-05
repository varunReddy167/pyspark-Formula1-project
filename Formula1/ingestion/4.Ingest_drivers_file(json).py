# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest drivers.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1: read the json file from dataframe reader api

# COMMAND ----------

from pyspark.sql.types import StructType,StringType,StructField,IntegerType,DateType

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename", StringType(),True),
                               StructField("surname", StringType(),True)
                               ])

# COMMAND ----------

drivers_schema=StructType(fields=[StructField("driverId", IntegerType(),False),
                               StructField("driverRef", StringType(),True),
                               StructField("number", IntegerType(),True),
                               StructField("code", StringType(),True),
                               StructField("name", name_schema),
                               StructField("dob", DateType(),True),
                               StructField("nationality", StringType(),True),
                               StructField("url", StringType(),True),
                                ])

# COMMAND ----------

drivers_df=spark.read\
    .schema(drivers_schema)\
    .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2: Rename columns and add new columns
# MAGIC 1.driverid\
# MAGIC 2.driverref\
# MAGIC 3.Ingestion date(add)\
# MAGIC 4.name added with concat of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col,concat,lit

# COMMAND ----------

drivers_withcolumns_df=add_ingestion(drivers_df)\
                                 .withColumnRenamed("driverId","driver_id")\
                                 .withColumnRenamed("driverRef","driver_ref")\
                                 .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))\
                                .withColumn("data_source",lit(v_data_source))\
                                 .withColumn("file_date",lit(v_file_date))   
                            


# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3: Drop unwanted columns
# MAGIC url

# COMMAND ----------

drivers_final_df=drivers_withcolumns_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4:write output to proccessed in parquet format

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers

# COMMAND ----------

dbutils.notebook.exit("Success")
