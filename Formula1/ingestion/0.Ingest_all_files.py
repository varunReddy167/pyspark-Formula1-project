# Databricks notebook source
v_result=dbutils.notebook.run("1.Ingest_Circuits_file",0,{"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("2.Ingest _Race_File",0,{"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("3.Ingest_constructors_file(json)",0,{"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("4.Ingest_drivers_file(json)",0,{"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("5.Ingest_results_files(json)",0,{"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("6.Ingest_pit_stops_file(json)-multiline",0,{"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("7.Ingest_lap_times_file_csv",0,{"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("8.Ingest_qualifying_file(json)-multiline",0,{"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result
