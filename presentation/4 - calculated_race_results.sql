-- Databricks notebook source
create table f1_presentation.calculated_race_results
using parquet
as
select races.race_year
        ,constructor.name as team_name
        ,drivers.name as driver_name
        ,results.points
        ,results.position
        ,(11-results.position) as calculated_points 
from f1_processed.results
  inner join f1_processed.drivers on (results.driver_id=drivers.driver_id)
  inner join f1_processed.constructor on (results.constructor_id=constructor.constructor_id)
  inner join f1_processed.races on (results.race_id=races.race_id)
where results.position <= 10

-- COMMAND ----------

select * from f1_presentation.calculated_race_results

-- COMMAND ----------


