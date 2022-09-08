# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using the spark dataframe reader

# COMMAND ----------

#import schema type functions from pyspark 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

#2nd:  Outer JSON - Driver Schema
lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                   StructField("driverId", IntegerType(), False),
                                   StructField("lap", IntegerType(), False),
                                   StructField("position", IntegerType(), False),
                                   StructField("time", StringType(), False),
                                   StructField("milliseconds", IntegerType(), True),
                                ])

#read data into dataframe
lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/lap_times/lap_times_split*.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename Columns

# COMMAND ----------

#renaming columns
from pyspark.sql.functions import lit
lap_times_renamed_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Transform Data

# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4 - Read data to datalake as parquet

# COMMAND ----------

#PartitionBy() partitions the data.  Similar to an index, helps with processing performance and splitting.
lap_times_final_df.write.mode("overwrite").partitionBy('race_id').parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

dbutils.notebook.exit("Success")
