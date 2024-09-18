-- Databricks notebook source
select team_name, count(*) as total_races, sum(calculated_points) as total_points, avg(calculated_points) as avg_points from f1_presentation.calculated_race_results
group by team_name
having total_races > 100  -- coz each team has 2 drivers
order by avg_points desc;

-- COMMAND ----------

select team_name, sum(calculated_points) as total_points, count(*) as total_races, avg(calculated_points) as avg_points from f1_presentation.calculated_race_results
where race_year between 2011 and 2020
group by team_name
having total_races > 100
order by avg_points desc;

-- COMMAND ----------


