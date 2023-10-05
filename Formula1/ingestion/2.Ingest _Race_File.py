# Databricks notebook source
# MAGIC %md
# MAGIC ##### step1: Read the csv file using the spark dataframe reader

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

from pyspark.sql.types import StructType,StructField, IntegerType,StringType,DateType

# COMMAND ----------

races_schema=StructType(fields=[StructField("raceId", IntegerType(),False),
                                  StructField("year", IntegerType(),True),
                                  StructField("round", IntegerType(),True),
                                  StructField("circuitId", IntegerType(),True),
                                  StructField("name", StringType(),True),
                                  StructField("date", DateType(),True),
                                  StructField("time", StringType(),True),
                                  StructField("url", StringType(),True),
])

# COMMAND ----------

races_df=spark.read\
    .option("header", True)\
    .schema(races_schema)\
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")


# COMMAND ----------

# MAGIC %md 
# MAGIC ####step 2: Add ingestion date and race_timestamp to dataframe

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,concat,lit,col

# COMMAND ----------

races_with_timestamp_df=add_ingestion(races_df)\
                                .withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '), col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3: Select only the required columns

# COMMAND ----------

races_selected_df=races_with_timestamp_df.select(col("raceId"),col("year"),col("round"),col("circuitId"),col("name"),col("ingestion_date"),col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4: Rename the columns as required

# COMMAND ----------

races_Renamed_df=races_selected_df.withColumnRenamed("raceId","race_id")\
                                      .withColumnRenamed("year","race_year")\
                                      .withColumnRenamed("circuitId","circuit_id")\
                                      .withColumn("data_source",lit(v_data_source))\
                                        .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #####step 5:Write the output data to datalake as parquet

# COMMAND ----------

races_Renamed_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races

# COMMAND ----------

dbutils.notebook.exit("Success")
