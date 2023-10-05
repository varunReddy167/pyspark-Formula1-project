-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style ="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Teams </h1?"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace TEMP VIEW v_dominant_Team 
AS
select Team_name,
   count(1) as total_races,
  sum(calculated_points) AS total_points,
  avg(calculated_points) AS Avg_points,
  rank() OVER(ORDER BY avg(calculated_points) desc) team_rank
  from f1_presentation.calculated_race_results
  group by Team_name
  having count(1)>=100
  order by Avg_points desc

-- COMMAND ----------

select * from v_dominant_Team 

-- COMMAND ----------

select race_year, Team_name,
   count(1) as total_races,
  sum(calculated_points) AS total_points,
  avg(calculated_points) AS Avg_points
  from f1_presentation.calculated_race_results
  WHERE Team_name in (select Team_name from v_dominant_Team where team_rank <=5)
  group by race_year,Team_name
  order by race_year, Avg_points desc

-- COMMAND ----------

select race_year, Team_name,
   count(1) as total_races,
  sum(calculated_points) AS total_points,
  avg(calculated_points) AS Avg_points
  from f1_presentation.calculated_race_results
  WHERE Team_name in (select Team_name from v_dominant_Team where team_rank <=5)
  group by race_year,Team_name
  order by race_year, Avg_points desc
