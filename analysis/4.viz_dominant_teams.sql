-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Teams </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_dominant_teams as
select team_name, sum(calculated_points) as total_points, count(*) as total_races, avg(calculated_points) as avg_points,
rank() over(order by avg(calculated_points) desc) as team_rank
from f1_presentation.calculated_race_results
group by team_name
having total_races > 100
order by avg_points desc;

-- COMMAND ----------

select * from v_dominant_teams

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####performance over the years

-- COMMAND ----------

select race_year, team_name, sum(calculated_points) as total_points, count(*) as total_races, avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where team_name in (select team_name from v_dominant_teams where team_rank <= 5)
group by race_year, team_name
-- having total_races > 50 not needed as in a year a driver wouldn't have participated in more than 20 races
order by race_year, avg_points desc;

-- COMMAND ----------


