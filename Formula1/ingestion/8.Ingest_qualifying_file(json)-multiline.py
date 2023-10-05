# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest qualifying__file

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
# MAGIC ####step 1 - read the Json file using dataframe reader api

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType


# COMMAND ----------

qualifying_schema=StructType(fields=[StructField("qualifyId", IntegerType(),False),
                              StructField("raceId", IntegerType(),True),
                              StructField("driverId", IntegerType(),True),
                              StructField("constructorId", IntegerType(),True),
                              StructField("number", IntegerType(),False),
                              StructField("position", IntegerType(),True),
                              StructField("q1", StringType(),True),
                              StructField("q2", StringType(),True),
                              StructField("q3", StringType(),True),
                               ])

# COMMAND ----------

qualifying_df=spark.read\
    .schema(qualifying_schema)\
    .option("multiline",True)\
    .json(f"{raw_folder_path}/{v_file_date}/qualifying/qualifying_split*.json") #using wildcard path * to list all files in folder

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2: Rename  columns and add new columns
# MAGIC 1.Rename driverid and raceid \
# MAGIC 2.Add ingestion_date with current Timestamp

# COMMAND ----------

from pyspark.sql.functions import lit
qualifying_final_df=add_ingestion(qualifying_df)\
                                .withColumnRenamed("raceId","race_id")\
                                 .withColumnRenamed("driverId","driver_id")\
                                 .withColumnRenamed("qualifyId","qualify_id")\
                                 .withColumnRenamed("constructorId","constructor_id")\
                                .withColumn("data_source",lit(v_data_source))\
                                 .withColumn("file_date",lit(v_file_date))
                               
                          

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: write the output to proccessed in parquet file

# COMMAND ----------

#qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

#overwrite_partition(qualifying_final_df,'f1_processed','qualifying','race_id')

# COMMAND ----------

merge_condition="tgt.qualify_id= src.qualify_id  AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final_df,'f1_processed','qualifying',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")
