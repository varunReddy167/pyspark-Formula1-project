# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest lap_times_file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 1 - read the csv file using dataframe reader api

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType


# COMMAND ----------

lap_times_schema=StructType(fields=[StructField("raceId", IntegerType(),False),
                              StructField("driverId", IntegerType(),False),
                              StructField("lap", IntegerType(),False),
                              StructField("position", IntegerType(),True),
                              StructField("time", StringType(),True),
                              StructField("milliseconds",IntegerType(),True)
                                ])

# COMMAND ----------

lap_times_df=spark.read\
    .schema(lap_times_schema)\
    .csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2: Rename  columns and add new columns
# MAGIC 1.Rename driverid and raceid \
# MAGIC 2.Add ingestion_date with current Timestamp

# COMMAND ----------

from pyspark.sql.functions import lit
lap_times_final_df=add_ingestion(lap_times_df)\
                               .withColumnRenamed("raceId","race_id")\
                               .withColumnRenamed("driverId","driver_id")\
                                .withColumn("data_source",lit(v_data_source))\
                                 .withColumn("file_date",lit(v_file_date))   
                            
                               
                          

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: write the output to proccessed in parquet file

# COMMAND ----------

#overwrite_partition(lap_times_final_df,'f1_processed','lap_times','race_id')

# COMMAND ----------

merge_condition="tgt.race_id= src.race_id  AND tgt.driver_id = src.driver_id  AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(lap_times_final_df,'f1_processed','lap_times',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")
