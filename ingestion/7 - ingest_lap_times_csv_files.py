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
.csv(f"{raw_folder_path}/{v_file_date}/lap_times/lap_times_split*.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename Columns

# COMMAND ----------

#renaming columns
lap_times_renamed_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Transform Data

# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_renamed_df).dropDuplicates(['race_id','driver_id','lap'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4 - Read data to datalake as parquet

# COMMAND ----------

#PartitionBy() partitions the data.  Similar to an index, helps with processing performance and splitting.
overwrite_partition_write_table(lap_times_final_df,'f1_processed','lap_times','race_id',['race_id','driver_id','lap'])

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


