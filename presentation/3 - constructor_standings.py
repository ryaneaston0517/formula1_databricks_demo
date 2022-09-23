# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_year_list = race_year_list_func(presentation_folder_path,"race_results",v_file_date)

# COMMAND ----------

df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

df1 = sum_points_and_group_by_func(df,["race_year","team"])

# COMMAND ----------

final_df = rank_by_year_func(df1)

# COMMAND ----------

overwrite_partition_write_table(final_df,'f1_presentation','constructor_standings','race_year')
