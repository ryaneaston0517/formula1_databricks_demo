-- Databricks notebook source
show databases

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use f1_processed;
show tables;

-- COMMAND ----------

select * from drivers

-- COMMAND ----------

desc drivers

-- COMMAND ----------

select name, dob as birth_date
from drivers
where nationality = 'British'
  and dob >= '1990-01-01'
order by dob desc;


-- COMMAND ----------

select *
from drivers
order by nationality, dob desc

-- COMMAND ----------

select name, nationality, dob as birth_date
from drivers
where (nationality = 'British' and dob >= '1990-01-01')
  or nationality = 'Indian'
order by dob desc;


-- COMMAND ----------


