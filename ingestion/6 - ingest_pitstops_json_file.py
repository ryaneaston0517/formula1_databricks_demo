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
# MAGIC #### Step 1 - Read the json file using the spark dataframe reader

# COMMAND ----------

#import schema type functions from pyspark 
#2nd:  Outer JSON - Driver Schema
pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                   StructField("driverId", IntegerType(), False),
                                   StructField("stop", IntegerType(), False),
                                   StructField("lap", IntegerType(), False),
                                   StructField("time", StringType(), False),
                                   StructField("duration", StringType(), True),
                                   StructField("milliseconds", IntegerType(), True),
                                ])

#read data into dataframe
pit_stops_df = spark.read.option("header",True).option("multiline", True) \
.schema(pit_stops_schema) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename Columns

# COMMAND ----------

#renaming columns
pit_stops_renamed_df = pit_stops_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Transform Data

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(pit_stops_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4 - Read data to datalake as delta

# COMMAND ----------

#function in the common functions notebook
overwrite_partition_write_table(pit_stops_final_df,'f1_processed','pit_stops','race_id',['race_id','driver_id','stop'])

# COMMAND ----------

dbutils.notebook.exit("Success")
