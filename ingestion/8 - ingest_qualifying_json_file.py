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
qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), False),
                                      StructField("constructorId", IntegerType(), False),
                                      StructField("number", StringType(), False),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                ])

#read data into dataframe
qualifying_df = spark.read \
.option("multiline", True) \
.schema(qualifying_schema) \
.json(f"{raw_folder_path}/qualifying/qualifying_split*.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename Columns

# COMMAND ----------

#renaming columns
from pyspark.sql.functions import lit
qualifying_renamed_df = qualifying_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("qualifyId", "qualify_id") \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Transform Data

# COMMAND ----------

lap_times_final_df = add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4 - Read data to datalake as parquet

# COMMAND ----------

#PartitionBy() partitions the data.  Similar to an index, helps with processing performance and splitting.
folder_name = "qualifying"
lap_times_final_df.write.mode("overwrite").partitionBy('race_id').parquet(f"{processed_folder_path}/{folder_name}")

# COMMAND ----------

dbutils.notebook.exit("Success")
