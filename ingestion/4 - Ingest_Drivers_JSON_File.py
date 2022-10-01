# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - - Read the JSON file using the spark dataframe reader

# COMMAND ----------

#import schema type functions from pyspark 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

#in this casse, we'll need to define 2 schemas because we have 2 json objects within the file.  First inner json - name schema:
name_schema = StructType(fields=[StructField("forename", StringType(), True),                                   
                                 StructField("surname", StringType(), True)
                                ])

#2nd:  Outer JSON - Driver Schema
driver_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                   StructField("driverRef", StringType(), True),
                                   StructField("number", IntegerType(), True),
                                   StructField("code", StringType(), True),
                                   StructField("name", name_schema, True),
                                   StructField("dob", DateType(), True),
                                   StructField("nationality", StringType(), True),
                                   StructField("url", StringType(), True),
                                ])

# COMMAND ----------

#read data into dataframe
drivers_df = spark.read.option("header",True) \
.schema(driver_schema) \
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename Columns

# COMMAND ----------

#renaming columns
from pyspark.sql.functions import col, concat, lit
drivers_renamed_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("driverRef", "driver_ref") \
.withColumn("name", concat(col("name.forename"),lit(" "),col("name.surname"))) \
.drop(col("forename")) \
.drop(col("surname")) \
.drop(col("url")) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Transform Data

# COMMAND ----------

drivers_final_df = add_ingestion_date(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4 - Read data to datalake as parquet

# COMMAND ----------

folder_name = "drivers"
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")
