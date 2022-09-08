# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename Columns

# COMMAND ----------

#renaming columns
from pyspark.sql.functions import col
constructors_renamed_df = constructors_df.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("constructorRef", "constructor_ref") \
.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Transform Data

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4 - Read data to datalake as parquet

# COMMAND ----------

folder_name = "constructor"
constructors_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/{folder_name}")

# COMMAND ----------

dbutils.notebook.exit("Success")
