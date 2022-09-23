# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
.filter("circuit_id < 70") \
.withColumnRenamed("name", "circuit_name") \
.withColumnRenamed("location", "circuit_location") \
.withColumnRenamed("race_country", "circuit_race_country")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
.filter("race_year = 2019") \
.withColumnRenamed("name","race_name") \
.withColumnRenamed("round","race_round")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

#inner join with selecting appropriate fields
race_circuits_inner_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(circuits_df.circuit_name, circuits_df.circuit_location, circuits_df.circuit_race_country, races_df.race_name, races_df.race_round)

# COMMAND ----------

display(race_circuits_inner_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Outer Join

# COMMAND ----------

# left join
race_circuits_left_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
.select(circuits_df.circuit_name, circuits_df.circuit_location, circuits_df.circuit_race_country, races_df.race_name, races_df.race_round)

# COMMAND ----------

display(race_circuits_left_df)

# COMMAND ----------

# right join
race_circuits_right_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
.select(circuits_df.circuit_name, circuits_df.circuit_location, circuits_df.circuit_race_country, races_df.race_name, races_df.race_round)

# COMMAND ----------

display(race_circuits_right_df)

# COMMAND ----------

# full outer join
race_circuits_full_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
.select(circuits_df.circuit_name, circuits_df.circuit_location, circuits_df.circuit_race_country, races_df.race_name, races_df.race_round)

# COMMAND ----------

display(race_circuits_full_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Semi Joins - similar to inner join - only given columns from left dataframe.

# COMMAND ----------

# semi join
race_circuits_semi_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi") \
#.select(circuits_df.circuit_name, circuits_df.circuit_location, circuits_df.circuit_race_country)
        #, races_df.race_name, races_df.race_round) <-- Cannot include these columns.  Will result in error

# COMMAND ----------

display(race_circuits_semi_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Anit-join - opposite of semi join - Everything on the left datafram that isn't found on the right dataframe

# COMMAND ----------

# semi join
race_circuits_anti_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti") \
#.select(circuits_df.circuit_name, circuits_df.circuit_location, circuits_df.circuit_race_country)
        #, races_df.race_name, races_df.race_round) <-- Cannot include these columns.  Will result in error

# COMMAND ----------

display(race_circuits_anti_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cross Joins - Gives cartesian product

# COMMAND ----------

race_circuits_cross_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuits_cross_df)

# COMMAND ----------


