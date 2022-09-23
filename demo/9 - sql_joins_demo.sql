-- Databricks notebook source
use f1_presentation

-- COMMAND ----------

desc driver_standings

-- COMMAND ----------

create or replace temp view v_driver_standings_2018
as
select race_year, driver_name, team, total_points, wins, rank
from driver_standings
where race_year = 2018

-- COMMAND ----------

select * from v_driver_standings_2018

-- COMMAND ----------

create or replace temp view v_driver_standings_2020
as
select race_year, driver_name, team, total_points, wins, rank
from driver_standings
where race_year = 2020

-- COMMAND ----------

--inner join
select *
from v_driver_standings_2018 d_2018
    join v_driver_standings_2020 d_2020
    on d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

--left join
select *
from v_driver_standings_2018 d_2018
    left join v_driver_standings_2020 d_2020
    on d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

--right join
select *
from v_driver_standings_2018 d_2018
    right join v_driver_standings_2020 d_2020
    on d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

--full join
select *
from v_driver_standings_2018 d_2018
    full join v_driver_standings_2020 d_2020
    on d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

--semi join - inner join with data only from the left table
select *
from v_driver_standings_2018 d_2018
    semi join v_driver_standings_2020 d_2020
    on d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

--anti join -- leftover from semi join
select *
from v_driver_standings_2018 d_2018
    anti join v_driver_standings_2020 d_2020
    on d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

--cross join - cartesian product - useful in joining small dim tables to fact table
select *
from v_driver_standings_2018 d_2018
    cross join v_driver_standings_2020 d_2020
   -- on d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------


