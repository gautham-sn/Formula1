-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Dominant Drivers Over The Decade

-- COMMAND ----------

select driver_name, count(*) as total_races, sum(calculated_points) as total_points, avg(calculated_points) as avg_points from f1_presentation.calculated_race_results
group by driver_name
having total_races > 50
order by avg_points desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Dominant Drivers In The Last Decade

-- COMMAND ----------

select driver_name, count(*) as total_races, sum(calculated_points) as total_points, avg(calculated_points) as avg_points from f1_presentation.calculated_race_results
where race_year between 2011 and 2020
group by driver_name
having total_races > 50
order by avg_points desc;

-- COMMAND ----------


