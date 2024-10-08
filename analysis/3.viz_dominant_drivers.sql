-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_dominant_drivers as
select driver_name, sum(calculated_points) as total_points, count(*) as total_races, avg(calculated_points) as avg_points,
rank() over(order by avg(calculated_points) desc) as driver_rank
from f1_presentation.calculated_race_results
group by driver_name
having total_races > 50
order by avg_points desc;

-- COMMAND ----------

select * from v_dominant_drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Top 10 Ranked Driver's Performance Over The Years

-- COMMAND ----------

select race_year, driver_name, sum(calculated_points) as total_points, count(*) as total_races, avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from v_dominant_drivers where driver_rank <= 10)
group by race_year, driver_name
-- having total_races > 50 not needed as in a year a driver wouldn't have participated in more than 20 races
order by race_year, avg_points desc;

-- COMMAND ----------


