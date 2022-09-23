-- Databricks notebook source
create database if not exists f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create circuits table

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits
(circuitId int,
circuitRef string,
location string,
country string,
lat double,
lng double,
alt int,
url string
)
using csv
options (path "/mnt/sadatabrickstutorial/raw/circuits.csv", header true);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Races Table

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races
(raceId int,
year int,
round int,
circuitId int,
name string,
date timestamp,
time string,
url string
)
using csv
options (path "/mnt/sadatabrickstutorial/raw/races.csv", header true);

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create tables for json files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create constructors table
-- MAGIC * Single Live JSON
-- MAGIC * Single Structure

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors
(
constructorId INT, 
constructorRef STRING, 
name STRING, 
nationality STRING, 
url STRING
)
using json
options (path "/mnt/sadatabrickstutorial/raw/constructors.json");

-- COMMAND ----------

select * from f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create drivers table
-- MAGIC * Single Line JSON
-- MAGIC * Complex structures

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers
(
driverId int,
driverRef string,
number int,
code string,
name struct<forename:string, surname:string>,
dob date,
nationality string,
url string
)
using json
options (path "/mnt/sadatabrickstutorial/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create results Table
-- MAGIC * Single Line JSON
-- MAGIC * Simple Structure

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results
(
resultId int,
driverId int,
constructorId int,
number int,
grid int,
position int,
positionText string,
positionOrder int,
points float,
laps int,
time string,
milliseconds int,
fastestLap int,
rank int,
fastestLapTime string,
fastestLapSpeed string,
statusId int
)
using json
options (path "/mnt/sadatabrickstutorial/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create pit stops table
-- MAGIC * Multi line JSON
-- MAGIC * Simple Structure

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops
(
driverId int,
duration string,
lap int,
milliseconds int,
raceId int,
stop int,
time string
)
using json
options (path "/mnt/sadatabrickstutorial/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create tables from lists of files

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###### Create Lap Times Table
-- MAGIC * csv file
-- MAGIC * multiple files

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times
(
raceId int,
driverId int,
lap int,
position int,
time string,
milliseconds int
)
using csv
options (path "/mnt/sadatabrickstutorial/raw/lap_times")


-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Create Qualifying Table
-- MAGIC * json file
-- MAGIC * multiple line json
-- MAGIC * multiple files

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying
(
qualifyId int,
raceId int,
driverId int,
constructorId int,
number string,
position int,
q1 string,
q2 string,
q3 string
)
using json
options (path "/mnt/sadatabrickstutorial/raw/qualifying", multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------


