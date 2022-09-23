# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

#import schema type functions from pyspark 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

#We should never infer the schema.  It is resource expensive.  SparkPy has functionality to do this
#structType = row
#StructField = column
#logically:  The structure type of the rows is what is being applied to the columns
circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),                                     
                                     StructField("location", StringType(), True),                                    
                                     StructField("country", StringType(), True),                                    
                                     StructField("lat", DoubleType(), True),                                    
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),                                    
                                     StructField("url", StringType(), True),                                    
                                    ])

# COMMAND ----------

#Create circuits dataframe
#want to be able to move between different environments for notebooks
#house environment information in another notebook.
circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Select Only the requiered Columns

# COMMAND ----------

#selecting only the required columns from the data
#Method 1:  this method will not let you apply any column based functions
# Very simple.  Best is just selecting.  Not the best is further transformation is needed
# circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location","country","lat","lng","alt")

# COMMAND ----------

#Method 2
# circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name
#                                           , circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lng,circuits_df.alt)

# COMMAND ----------

#Method 3
# circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"]
#                                           , circuits_df["location"],circuits_df["country"],circuits_df["lat"],circuits_df["lng"],circuits_df["alt"])

# COMMAND ----------

#Method 4
# Preferred Method:  the col function makes is very easy to modify columns.
from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name")
                                          , col("location"),col("country").alias("race_country") #alias function = rename
                                          ,col("lat"),col("lng"),col("alt"))

# COMMAND ----------

# display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

#renaming columns
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuite_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

#current_timestamp:  adding object output into the new column
#env: adding a literal value / string to the new column - need the lit function
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - write data to datalake as parquet

# COMMAND ----------

#write.parquet will write to parquet files
#write.mode will dictate if you want to overwrite/append/error if file already exists.
#circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")
