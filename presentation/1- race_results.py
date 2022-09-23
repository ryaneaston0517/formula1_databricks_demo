# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.  Join Races and circuits data
# MAGIC Retain only
# MAGIC * races.race_year
# MAGIC * races.race_name
# MAGIC * races.race_date
# MAGIC * circuits.circuit_location

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
.withColumnRenamed("name","race_name") \
.withColumnRenamed('race_timestamp', 'race_date') \
.withColumnRenamed('file_date', 'races_file_date')
#.filter("circuit_id = 24 and race_year = 2020") \

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location","circuit_location") \
.withColumnRenamed("file_date","circuit_file_date")

# COMMAND ----------

#inner join with selecting appropriate fields
race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location, races_df.races_file_date)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join Results Data with race_circuits_df

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed("time","race_time") \
.withColumnRenamed("file_date","results_file_date")
#.filter("race_id = 1047")

# COMMAND ----------

#inner join with selecting appropriate fields
race_results_df = race_circuits_df.join(results_df, races_df.race_id == results_df.race_id, "inner") \
.select(race_circuits_df.race_id, 
        race_circuits_df.race_year,
        race_circuits_df.race_name,
        race_circuits_df.race_date,
        race_circuits_df.circuit_location,
        results_df.result_id,
        results_df.driver_id,
        results_df.constructor_id,
        results_df.grid,
        results_df.fastest_lap,
        results_df.race_time,
        results_df.points,
        results_df.position,
        results_df.results_file_date
       )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join Driver information onto race_results_df

# COMMAND ----------

from pyspark.sql.functions import col
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
.withColumnRenamed("name","driver_name") \
.withColumnRenamed("number","driver_number") \
.withColumnRenamed("nationality", "driver_nationality") \
.drop(col("driver_ref")) \
.drop(col("code")) \
.drop(col("dob")) \
.drop(col("data_source")) \
.drop(col("ingestion_date")) \
.withColumnRenamed("file_date", "driver_file_date")

# COMMAND ----------

race_drivers_df = race_results_df.join(drivers_df, "driver_id", "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC #### join constructor data onto race_drivers_df

# COMMAND ----------

constructor_df = spark.read.parquet(f"{processed_folder_path}/constructor") \
.withColumnRenamed("name","team") \
.withColumnRenamed("file_date","constructor_file_date") \
.drop(col("constructor_ref")) \
.drop(col("nationality")) \
.drop(col("ingestion_date"))

# COMMAND ----------

race_constructor_df = race_drivers_df.join(constructor_df, "constructor_id", "inner") \
.drop(col("driver_id")) \
.drop(col("result_id")) \
.drop(col("constructor_id"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = race_constructor_df.withColumn("created_date", current_timestamp()) \
.drop(col("constructor_file_date")) \
.drop(col("driver_file_date")) \
.orderBy(race_constructor_df.points.desc()) \
.withColumnRenamed("results_file_date","file_date")

# COMMAND ----------

overwrite_partition_write_table(final_df,'f1_presentation','race_results','race_id')

# COMMAND ----------


