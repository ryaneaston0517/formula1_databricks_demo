# Databricks notebook source
# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results

# COMMAND ----------

#local temp view - only needed within a single notebook
#global temp view - other notebooks are dependent on the temp view that has been generated
