# Databricks notebook source
from pyspark.sql.functions import current_timestamp, desc, rank, sum, when, count, col, lit, concat_ws
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def rearrange_partition_column(input_df, partition_column):
    select_list = input_df.schema.names
    select_list.append(select_list.pop(select_list.index(partition_column)))
    output_df = input_df.select(select_list)
    return output_df

# COMMAND ----------

def overwrite_partition_write_table(input_df,db_name,table_name,partition_column,delta_merge_key_list):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
    
    if db_name == 'f1_processed':
        blob_layer = 'processed'
    else:
        blob_layer = 'presentation'
    
    tmp_df = input_df.withColumn('delta_merge_key',concat_ws("-",*delta_merge_key_list))
    
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    output_df = rearrange_partition_column(tmp_df,partition_column)
    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
        deltaTable = DeltaTable.forPath(spark, f"dbfs:/mnt/sadatabrickstutorial/{blob_layer}/{table_name}/")
        deltaTable.alias('tgt').merge(output_df.alias("src"),
                                     f"tgt.delta_merge_key = src.delta_merge_key and tgt.{partition_column} =src.{partition_column}") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        output_df.write.mode("append").partitionBy(f"{partition_column}").format("delta").saveAsTable(f"{db_name}.{table_name}")
    return None

# COMMAND ----------

def race_year_list_func(blob_path,blob_file,file_date):
    output_df = spark.read.format("delta").load(f"{blob_path}/{blob_file}") \
    .filter(f"file_date = '{file_date}'") \
    .select("race_year").distinct() \
    .collect()
    
    race_year_list = []
    for race_year in output_df:
        race_year_list.append(race_year.race_year)
    return race_year_list

# COMMAND ----------

def rank_by_year_func(input_df):
    #In order to rank by year, we need to leverage the Window function.
    #define the Window Function
    constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

    #Create rank column by applying the Window function above using the over() function
    output_df = input_df.withColumn("rank",rank().over(constructor_rank_spec))

    #TODO:  Research Window Functions: A window (windowspec) is just a window definition.
    # Above: The window is partitioned by the race yaer and orders by total points and then wins... by race year.
    return output_df

# COMMAND ----------

def sum_points_and_group_by_func(input_df,group_by_list):
    output_df = input_df \
    .groupBy(group_by_list) \
    .agg(sum("points").alias("total_points"),
        count(when(col("position") == 1, True)).alias("wins") 
        )
    #essentially performas a boolean sql case when.  When positon = 1 then True.  Counting Trues 
    return output_df
