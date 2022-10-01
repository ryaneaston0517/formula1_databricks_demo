# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using the spark dataframe reader

# COMMAND ----------

#import schema type functions from pyspark 
#2nd:  Outer JSON - Driver Schema
results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                   StructField("raceId", IntegerType(), True),
                                   StructField("driverId", IntegerType(), True),
                                   StructField("constructorId", IntegerType(), True),
                                   StructField("number", IntegerType(), True),
                                   StructField("grid", IntegerType(), True),
                                   StructField("position", IntegerType(), True),
                                   StructField("positionText", StringType(), True),
                                   StructField("positionOrder", IntegerType(), True),
                                   StructField("points", FloatType(), True),
                                   StructField("laps", IntegerType(), True),
                                   StructField("time", StringType(), True),
                                   StructField("milliseconds", IntegerType(), True),
                                   StructField("fastestLap", IntegerType(), True),
                                   StructField("rank", IntegerType(), True),
                                   StructField("fastestLapTime", StringType(), True),
                                   StructField("fastestLapSpeed", StringType(), True),
                                   StructField("statusId", IntegerType(), True)
                                ])

#read data into dataframe
results_df = spark.read.option("header",True) \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename Columns

# COMMAND ----------

#renaming columns
results_renamed_df = results_df.withColumnRenamed("resultId", "result_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("positionText", "position_text") \
.withColumnRenamed("positionOrder", "position_order") \
.withColumnRenamed("fastestLap", "fastest_lap") \
.withColumnRenamed("fastestLapTime", "fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
.drop(col("statusId")) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Transform Data

# COMMAND ----------

results_final_df = add_ingestion_date(results_renamed_df).dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4 - Read data to datalake as delta

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Drop Partition if it exists

# COMMAND ----------

#method 1 - purge existing data

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if spark._jsparkSession.catalog().tableExists("f1_processed.results"):
#         spark.sql(f"alter table f1_processed.results drop if exists partition (race_id = {race_id_list.race_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write to parquet

# COMMAND ----------

#function in the common functions notebook
overwrite_partition_write_table(results_final_df,'f1_processed','results','race_id',['result_id'])

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


