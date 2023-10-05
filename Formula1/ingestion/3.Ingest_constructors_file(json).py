# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest_constructors_file(json)

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
# MAGIC ####step 1- Read the JSON file using dataframe reader

# COMMAND ----------

constructors_schema="constructorId INT, constructorRef STRING,name STRING,nationality STRING,url STRING"

# COMMAND ----------

constructor_df=spark.read\
    .schema(constructors_schema)\
    .json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2:Drop unwanted columns from the dataframe

# COMMAND ----------

constructor_dropped_df=constructor_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3: Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import lit
constructor_final_df=add_ingestion(constructor_dropped_df)\
                  .withColumnRenamed("constructorId","constructor_id")\
                  .withColumnRenamed("constructorRef","constructor_ref")\
                 .withColumn("data_source",lit(v_data_source))\
                  .withColumn("file_date",lit(v_file_date))   
                                            
                                           

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Write output into parquet file
# MAGIC

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors

# COMMAND ----------

dbutils.notebook.exit("Success")
