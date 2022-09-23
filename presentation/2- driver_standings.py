# Databricks notebook source
# MAGIC %md
# MAGIC #### Produce Driver Standings

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Find race year for which the data is to be reprocessed.  Analysis happens on race year.

# COMMAND ----------

race_year_list = race_year_list_func(presentation_folder_path,"race_results",v_file_date)

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

driver_standings_df = sum_points_and_group_by_func(race_results_df,["race_year", "driver_name", "driver_nationality", "team"])

# COMMAND ----------

final_df = rank_by_year_func(driver_standings_df)

# COMMAND ----------

overwrite_partition_write_table(final_df,'f1_presentation','driver_standings','race_year')
