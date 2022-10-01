# Databricks notebook source
# MAGIC %md
# MAGIC 1.  Write data to the delta lake (managed table)
# MAGIC 2.  Write data to delta lake (external table)
# MAGIC 3.  Read data from delta lake (table)
# MAGIC 4.  Read data from delta lake (file)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_demo
# MAGIC location 'dbfs:/mnt/sadatabrickstutorial/demo/'

# COMMAND ----------

results_df = spark.read \
.option("inferSchema",True) \
.json("/mnt/sadatabrickstutorial/raw/2021-03-28/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

#write to managed table
results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

#write to external table
results_df.write.format("delta").mode("overwrite").save('dbfs:/mnt/sadatabrickstutorial/demo/results_external')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table f1_demo.results_external
# MAGIC using delta
# MAGIC location 'dbfs:/mnt/sadatabrickstutorial/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

#read in external delta file
results_external_df = spark.read.format("delta").load("dbfs:/mnt/sadatabrickstutorial/demo/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

#create a partition table
results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_managed_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC --show partitions
# MAGIC show partitions f1_demo.results_managed_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC 1.  Update Delta Table
# MAGIC 2.  Delete from Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_demo.results_managed
# MAGIC set points=11-position
# MAGIC where position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, 'dbfs:/mnt/sadatabrickstutorial/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "position <= 10",
  set = { "points": "21 - position" }
)

# Declare the predicate by using Spark SQL functions.
# deltaTable.update(
#   condition = col('gender') == 'M',
#   set = { 'gender': lit('Male') }
# )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from  f1_demo.results_managed
# MAGIC where position > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, 'dbfs:/mnt/sadatabrickstutorial/demo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete("points = 0")

# # Declare the predicate by using Spark SQL functions.
# deltaTable.delete(col('birthDate') < '1960-01-01')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Upsert using merge

# COMMAND ----------

drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json("dbfs:/mnt/sadatabrickstutorial/raw/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json("dbfs:/mnt/sadatabrickstutorial/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 6 and 15") \
.select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json("dbfs:/mnt/sadatabrickstutorial/raw/2021-03-28/drivers.json") \
.filter("(driverId BETWEEN 1 and 5) or (driverId between 16 and 20)") \
.select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

drivers_day3_df.createOrReplaceTempView("drivers_day3")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob date,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC createdDate date,
# MAGIC updatedDate date
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC             tgt.forename = upd.forename,
# MAGIC             tgt.surname = upd.surname,
# MAGIC             tgt.updatedDate = current_timestamp
# MAGIC when not matched then
# MAGIC   insert (driverId, dob, forename, surname, createdDate) values (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC             tgt.forename = upd.forename,
# MAGIC             tgt.surname = upd.surname,
# MAGIC             tgt.updatedDate = current_timestamp
# MAGIC when not matched then
# MAGIC   insert (driverId, dob, forename, surname, createdDate) values (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import current_timestamp
deltaTable = DeltaTable.forPath(spark, 'dbfs:/mnt/sadatabrickstutorial/demo/drivers_merge')

deltaTable.alias('tgt') \
  .merge(
    drivers_day3_df.alias('upd'),
    'tgt.driverId = upd.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "updatedDate": "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History & Versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vacuum

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge 
# MAGIC version as of 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge 
# MAGIC timestamp as of '2022-09-25T20:48:44.000+0000'

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", '2022-09-25T20:48:44.000+0000').load("/mnt/sadatabrickstutorial/demo/drivers_merge")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge 
# MAGIC timestamp as of '2022-09-25T20:48:44.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC vacuum f1_demo.drivers_merge
# MAGIC retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge 
# MAGIC timestamp as of '2022-09-25T20:48:44.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_merge 
# MAGIC where driverId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_merge 
# MAGIC where driverId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge
# MAGIC version as of 3

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo.drivers_merge tgt
# MAGIC using f1_demo.drivers_merge version as of 3 src
# MAGIC on tgt.driverId=src.driverId
# MAGIC when not matched then 
# MAGIC  insert *

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Transaction Logs

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_txn
# MAGIC (
# MAGIC dirverId int,
# MAGIC dob date,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC createdDate Date,
# MAGIC updatedDate date
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge
# MAGIC where driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge
# MAGIC where driverId = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_txn

# COMMAND ----------

for driver_id in range(3,20):
    spark.sql(
        f"""
        insert into f1_demo.drivers_txn
        select * from f1_demo.drivers_merge
        where driverId = {driver_id}
        """
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_convert_to_delta
# MAGIC (
# MAGIC driverId int,
# MAGIC dob date,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC createdDate date,
# MAGIC updatedDate date
# MAGIC )
# MAGIC using parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_convert_to_delta
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta f1_demo.drivers_convert_to_delta

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/sadatabrickstutorial/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta parquet.`/mnt/sadatabrickstutorial/demo/drivers_convert_to_delta_new`

# COMMAND ----------


