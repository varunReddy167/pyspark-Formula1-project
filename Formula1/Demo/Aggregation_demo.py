# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Built-in Aggregation functions

# COMMAND ----------

race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_agg_df=race_results_df.filter("race_year==2020")

# COMMAND ----------

display(demo_agg_df)

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct,sum

# COMMAND ----------

demo_agg_df.select(count("*")).show()

# COMMAND ----------

demo_agg_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_agg_df.select(sum("points")).show()

# COMMAND ----------

demo_agg_df.groupBy("driver_name").agg(sum("points").alias("total_points") ,countDistinct("race_name").alias("number_of_races")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####window Functions

# COMMAND ----------

demo_agg_df=race_results_df.filter("race_year in(2019,2020)")

# COMMAND ----------

display(demo_agg_df)

# COMMAND ----------

demo_grouped_df=demo_agg_df.groupBy("race_year","driver_name").agg(sum("points").alias("total_points") ,countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank
driver_rank=Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grouped_df.withColumn("rank", rank().over(driver_rank)).show(100)


# COMMAND ----------


