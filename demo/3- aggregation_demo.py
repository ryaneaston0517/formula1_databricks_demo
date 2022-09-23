# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregate functions demo

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Built_in Aggregate functions

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results").filter("race_year in (2019,2020)")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

race_results_df.select(count("*")).show()

# COMMAND ----------

race_results_df.select(count("race_name")).show()

# COMMAND ----------

race_results_df.select(countDistinct("race_name")).show()

# COMMAND ----------

race_results_df.filter("driver_name = 'Lewis Hamilton'") \
.select(sum("points"), countDistinct("race_name")) \
.withColumnRenamed("sum(points)","total_points") \
.withColumnRenamed("count(DISTINCT race_name)","number_of_races") \
.show()

# COMMAND ----------

#agg function allows us to do multiple aggregations in place
#alias function is a quick and easy rename of a column
from pyspark.sql.functions import col
race_results_grouped_df = race_results_df \
.groupBy("race_year", "driver_name") \
.agg(sum("points").alias("total_points")
     , countDistinct("race_name").alias("number_of_races")
    ) \
.orderBy(col("total_points").desc(), col("number_of_races").desc()) \
#.show()

# COMMAND ----------

#partition by race_year
#order by total_points descending
from pyspark.sql.window import Window
from pyspark.sql.functions import desc
driverRankSpec = Window.partitionBy("race_year") \
.orderBy(desc("total_points"))

# COMMAND ----------

#add a rank column - use window function


# COMMAND ----------

from pyspark.sql.functions import rank
race_results_grouped_df.withColumn("rank", rank().over(driverRankSpec)).show()

#TODO:  understand the over function - defines windowing column.

# COMMAND ----------


