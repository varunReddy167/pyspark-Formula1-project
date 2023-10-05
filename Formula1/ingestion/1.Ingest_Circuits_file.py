# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest circuits.csv file

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
# MAGIC ##### step1: Read the csv file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, IntegerType,StringType,DoubleType

# COMMAND ----------

circuit_schema=StructType(fields=[StructField("circuitId", IntegerType(),False),
                                  StructField("circuitRef", StringType(),True),
                                  StructField("name", StringType(),True),
                                  StructField("location", StringType(),True),
                                  StructField("country", StringType(),True),
                                  StructField("lat", DoubleType(),True),
                                  StructField("lng", DoubleType(),True),
                                  StructField("alt", IntegerType(),True),
                                  StructField("url", StringType(),True),
])

# COMMAND ----------

circuits_df=spark.read\
    .option("header", True)\
    .schema(circuit_schema)\
    .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2: Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

circuit_selected_df=circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3: Rename the columns as required

# COMMAND ----------

circuits_Renamed_df=circuit_selected_df.withColumnRenamed("circuitId","circuit_Id")\
                                      .withColumnRenamed("circuitRef","circuit_Ref")\
                                      .withColumnRenamed("lat","latitude")\
                                      .withColumnRenamed("lng","longitude")\
                                      .withColumnRenamed("alt","altitude")\
                                      .withColumn("data_source",lit(v_data_source))\
                                       .withColumn("file_date",lit(v_file_date))
                                    
                                          
                                       

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4: Add ingest date to the dataframe

# COMMAND ----------

circuits_finla_df=add_ingestion(circuits_Renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####step 5:Write data to datalake as parquet

# COMMAND ----------

circuits_finla_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit("Success")
