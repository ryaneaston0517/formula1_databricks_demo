# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the json file using the spark dataframe reader

# COMMAND ----------

#import schema type functions from pyspark 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

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
.json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename Columns

# COMMAND ----------

#renaming columns
from pyspark.sql.functions import lit
pit_stops_renamed_df = pit_stops_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Transform Data

# COMMAND ----------

pit_stops_final_df = add_ingestion_date(pit_stops_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4 - Read data to datalake as parquet

# COMMAND ----------

#PartitionBy() partitions the data.  Similar to an index, helps with processing performance and splitting.
pit_stops_final_df.write.mode("overwrite").partitionBy('race_id').parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Success")
