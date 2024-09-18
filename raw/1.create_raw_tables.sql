-- Databricks notebook source
create database if not exists f1_raw;

-- COMMAND ----------

use f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create circuits table

-- COMMAND ----------

drop table if exists circuits;
create table if not exists circuits(
  circuitId int,
  circuitRef string,
  name string,
  location string,
  country string,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url string
)
using csv 
options(path "/mnt/formula1dl15/raw/circuits.csv", header true);

-- COMMAND ----------

select * from circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create races table

-- COMMAND ----------

drop table if exists races;
create table races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
using csv
options(path "/mnt/formula1dl15/raw/races.csv", header true)

-- COMMAND ----------

select * from races;

-- COMMAND ----------

-- MAGIC  %md
-- MAGIC ##### Create constructors table

-- COMMAND ----------

drop table if exists constructors;
create table if not exists constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
using json
options(path "/mnt/formula1dl15/raw/constructors.json")

-- COMMAND ----------

select * from constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create drivers table

-- COMMAND ----------

drop table if exists drivers;
create table if not exists drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name struct < forename : string, surname : string>,
  dob DATE,
  nationality STRING,
  url STRING
)
using json
options(path "/mnt/formula1dl15/raw/drivers.json")

-- COMMAND ----------

select * from drivers;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Create results table

-- COMMAND ----------

DROP TABLE IF EXISTS results;
CREATE TABLE IF NOT EXISTS results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points INT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId STRING
)
USING json
OPTIONS(path "/mnt/formula1dl15/raw/results.json")

-- COMMAND ----------

select * from results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create pit stops table

-- COMMAND ----------

drop table if exists pit_stops;
create table if not exists pit_stops(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
using json
options(path "/mnt/formula1dl15/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

select * from pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Lap Times Table

-- COMMAND ----------

drop table if exists lap_times;
create table if not exists lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
using csv
options(path "/mnt/formula1dl15/raw/lap_times")

-- COMMAND ----------

select * from lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Qualifying Table

-- COMMAND ----------

drop table if exists qualifying;
create table if not exists qualifying(
  constructorId INT,
  driverId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  qualifyId INT,
  raceId INT
)
using json
options(path "/mnt/formula1dl15/raw/qualifying", multiLine true)

-- COMMAND ----------

select * from qualifying;

-- COMMAND ----------

describe extended qualifying;

-- COMMAND ----------


