# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest pit_stops_file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 1 - read the Json file using dataframe reader api

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType


# COMMAND ----------

pit_schema=StructType(fields=[StructField("raceId", IntegerType(),False),
                              StructField("driverId", IntegerType(),False),
                              StructField("stop", StringType(),True),
                              StructField("lap", IntegerType(),False),
                              StructField("time", StringType(),True),
                              StructField("duration", StringType(),True),
                              StructField("milliseconds",IntegerType(),True)
                                ])

# COMMAND ----------

pit_stops_df=spark.read\
    .schema(pit_schema)\
    .option("multiline",True)\
    .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2: Rename  columns and add new columns
# MAGIC 1.Rename driverid and raceid \
# MAGIC 2.Add ingestion_date with current Timestamp

# COMMAND ----------

from pyspark.sql.functions import lit
pit_stops_final_df=add_ingestion(pit_stops_df)\
                            .withColumnRenamed("raceId","race_id")\
                            .withColumnRenamed("driverId","driver_id")\
                            .withColumn("data_source",lit(v_data_source))\
                             .withColumn("file_date",lit(v_file_date))  
                            
                               
                          

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: write the output to proccessed in parquet file

# COMMAND ----------

#overwrite_partition(pit_stops_final_df,'f1_processed','pit_stops','race_id')

# COMMAND ----------

merge_condition="tgt.race_id= src.race_id  AND tgt.driver_id = src.driver_id  AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pit_stops_final_df,'f1_processed','pit_stops',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pit_stops
