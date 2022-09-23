-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Lesson Objectives
-- MAGIC 1. Spark sql documentation
-- MAGIC 2. Create database demo
-- MAGIC 3. data tabe in the ui
-- MAGIC 4. show command
-- MAGIC 5. describe command
-- MAGIC 6. find the current database

-- COMMAND ----------

create database if not exists demo;

-- COMMAND ----------

show databases

-- COMMAND ----------

describe database extended demo

-- COMMAND ----------

select current_database();

-- COMMAND ----------

use demo

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

show tables

-- COMMAND ----------

use default

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Managed Tables

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

describe extended race_results_python

-- COMMAND ----------

select *
from demo.race_results_python
where race_year = 2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### create managed table using sql

-- COMMAND ----------

create table demo.race_results_sql
as
select *
from demo.race_results_python
where race_year = 2020

-- COMMAND ----------

select * from demo.race_results_sql

-- COMMAND ----------

drop table demo.race_results_sql;

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create External Tables
-- MAGIC 1.  Create external table using python
-- MAGIC 2.  create external table using sql
-- MAGIC 3.  Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #create managed table using python
-- MAGIC #race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #the only difference between creating a managed table and an external table is setting an option
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_python").saveAsTable("demo.race_results_python_ext_python")

-- COMMAND ----------

desc extended demo.race_results_python_ext_python

-- COMMAND ----------



-- COMMAND ----------

drop table if exists demo.race_results_ext_sql;

create table demo.race_results_ext_sql
(race_year int,
race_name string,
race_date timestamp,
circuit_location string,
grid int,
fastest_lap int,
race_time string,
points float,
position int,
driver_name string,
driver_number string,
driver_nationality string,
team string,
created_date timestamp
)
using parquet
location "/mnt/formuladl/presentation/race_results_ext_sql";

-- COMMAND ----------

insert into demo.race_results_ext_sql
select * from demo.race_results_python_ext_python where race_year = 2020;

-- COMMAND ----------

select  count(1) from demo.race_results_ext_sql

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

drop table race_results_ext_sql

-- COMMAND ----------

--dropping table only disconnects the databricks instance from the external connection
--managed tables:  spark maintains metadata and data
--external tables:  spark maintains metadata, users maintain data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Views on tables
-- MAGIC 1.  Create temp view - only available to the spark notebook
-- MAGIC 2. Create Global Temp View - any notebook attached to the cluster can access
-- MAGIC 3.  Create Permanent View

-- COMMAND ----------

create or replace temp view v_race_results
as
select *
from demo.race_results_python
where race_year = 2018;

-- COMMAND ----------

select * from v_race_results

-- COMMAND ----------

create or replace global temp view gv_race_results
as
select *
from demo.race_results_python
where race_year = 2012;

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

--permanent view
create or replace view demo.pv_race_results
as
select *
from demo.race_results_python
where race_year = 2000;

-- COMMAND ----------

show tables

-- COMMAND ----------


