-- Databricks notebook source
--calculate total points by driver and then rank
select driver_name
      , count(*) as total_races
      , sum(calculated_points) as total_points
      , avg(calculated_points) as avg_points
      , rank() over(order by avg(calculated_points) desc) as driver_rank
from f1_presentation.calculated_race_results
where race_year between 2011 and 2020
group by driver_name
having total_races > 50
order by avg_points desc

-- COMMAND ----------

--calculate total points by driver and then rank
select driver_name
      , count(*) as total_races
      , sum(calculated_points) as total_points
      , avg(calculated_points) as avg_points
      , rank() over(order by avg(calculated_points) desc) as driver_rank
from f1_presentation.calculated_race_results
where race_year between 2001 and 2010
group by driver_name
having total_races > 50
order by avg_points desc

-- COMMAND ----------

--most dominant teams of all time
select team_name
      , count(*) as total_races
      , sum(calculated_points) as total_points
      , avg(calculated_points) as avg_points
      , rank() over(order by avg(calculated_points) desc) as team_rank
from f1_presentation.calculated_race_results
--where race_year between 2011 and 2010
group by team_name
having total_races > 100
order by avg_points desc

-- COMMAND ----------

select team_name
      , count(*) as total_races
      , sum(calculated_points) as total_points
      , avg(calculated_points) as avg_points
      , rank() over(order by avg(calculated_points) desc) as team_rank
from f1_presentation.calculated_race_results
where race_year between 2010 and 2020
group by team_name
having total_races > 100
order by avg_points desc

-- COMMAND ----------

select team_name
      , count(*) as total_races
      , sum(calculated_points) as total_points
      , avg(calculated_points) as avg_points
      , rank() over(order by avg(calculated_points) desc) as team_rank
from f1_presentation.calculated_race_results
where race_year between 2001 and 2010
group by team_name
having total_races > 10
order by avg_points desc

-- COMMAND ----------


