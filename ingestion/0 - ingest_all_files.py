# Databricks notebook source
v_result = dbutils.notebook.run("1 - ingest_circuits_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2 - ingest_races_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3 - Ingest Constructors JSON File", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4 - Ingest_Drivers_JSON_File", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5 - ingest_results_json_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6 - ingest_pitstops_json_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7 - ingest_lap_times_csv_files", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8 - ingest_qualifying_json_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})
v_result
