-- Databricks notebook source
use f1_processed

-- COMMAND ----------

select *
      , concat(driver_ref,'-',code) as new_driver_reference 
from drivers

-- COMMAND ----------

select *, split(name, ' ')[0] as forename
        ,split(name, ' ')[1] as surname
from drivers

-- COMMAND ----------

select *, current_timestamp
from drivers

-- COMMAND ----------

select *, date_format(dob, 'dd-MM-yyyy')
from drivers

-- COMMAND ----------

select *, date_add(dob, 1)
from drivers

-- COMMAND ----------

select count(*)
from drivers

-- COMMAND ----------

select max(dob)
from drivers

-- COMMAND ----------

select * from drivers
where dob = '2000-05-11'

-- COMMAND ----------

select nationality, count(*) as count
from drivers
--where nationality = 'British'
group by nationality
having count > 100
order by count desc

-- COMMAND ----------

 --Window functions:  Good for partitioning
 select nationality, name, dob, rank() over(partition by nationality order by dob desc) as age_rank
 from drivers
 order by nationality, age_rank

-- COMMAND ----------


