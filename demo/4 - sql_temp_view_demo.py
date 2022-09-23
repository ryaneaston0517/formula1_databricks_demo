# Databricks notebook source
# MAGIC %md
# MAGIC #### Access dataframes using SQL
# MAGIC 
# MAGIC ###### Objectives
# MAGIC 1. Create temparary views on dataframs
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

#create a view on top of the dataframe
race_results_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC --access the view from sql cell
# MAGIC select count(1)
# MAGIC from v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

#run sql from python cell
race_results_2019_df = spark.sql("select * from v_race_results where race_year = 2019")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# Cannot pass in variable into sql cell
# Can pass in variable using spark sql
#temp view will not be available in another notebook.  Only available within a single spark session.

# COMMAND ----------

#replace temp view
race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Global Temp View
# MAGIC 
# MAGIC ###### Objectives
# MAGIC 1. Create global temparary views on dataframs
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from Python cell
# MAGIC 4. Access the view from another notebook

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gv_race_results
# MAGIC 
# MAGIC --error happens because the view is stored in a database called global.temp

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results

# COMMAND ----------

spark.sql("select * from global_temp.gv_race_results").show()

# COMMAND ----------


