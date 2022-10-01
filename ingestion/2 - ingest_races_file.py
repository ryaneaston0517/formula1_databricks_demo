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
# MAGIC #### 1 - Read in races csv

# COMMAND ----------

#import schema type functions from pyspark 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), False),
                                     StructField("round", IntegerType(), False),                                     
                                     StructField("circuitId", IntegerType(), False),                                    
                                     StructField("name", StringType(), False),                                    
                                     StructField("date", DateType(), False),                                    
                                     StructField("time", StringType(), False),                                    
                                     StructField("url", StringType(), True),                                    
                                    ])

# COMMAND ----------

#read data into dataframe
races_df = spark.read.option("header",True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2 - Rename Columns

# COMMAND ----------

#renaming columns
races_renamed_df = races_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3 - Transform Data:
# MAGIC * Create ingestion_date column using current_timestamp() function
# MAGIC * Create Race_timestamp column using the to_timestamp() and concat() functions:  to_timestamp(concat("string_1","string_2","string_3"))
# MAGIC * drop date column
# MAGIC * drop time column
# MAGIC * drop url column

# COMMAND ----------

#create ingestion date column with current timestamp
#create race_timestamp column using the to_timestamp and concat functions
#drop date column
#drop time column
#drop url column

from pyspark.sql.functions import lit, to_timestamp, concat, col
races_ingestion_date = add_ingestion_date(races_renamed_df)
races_final_df = races_ingestion_date \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss')) \
.drop(col('date')) \
.drop(col('time')) \
.drop(col("url")) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4 - Read data to datalake as parquet

# COMMAND ----------

#PartitionBy() partitions the data.  Similar to an index, helps with processing performance and splitting.
races_final_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")
