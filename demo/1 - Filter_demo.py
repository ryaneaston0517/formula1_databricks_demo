# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

#sql way
races_sql_filtered_df = races_df.filter("race_year = 2019 and round <= 5")

# COMMAND ----------

#python syntax
races_python_filtered_df = races_df.filter((races_df.race_year == 2019) & (races_df.round <= 5))

# COMMAND ----------

display(races_python_filtered_df)

# COMMAND ----------


