-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Drop All Tables

-- COMMAND ----------

drop database if exists f1_processed cascade

-- COMMAND ----------

create database if not exists f1_processed
location "/mnt/sadatabrickstutorial/processed"

-- COMMAND ----------

drop database if exists f1_presentation cascade

-- COMMAND ----------

create database if not exists f1_presentation
location "/mnt/sadatabrickstutorial/presentation"

-- COMMAND ----------


