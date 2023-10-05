-- Databricks notebook source
select Team_name,
   count(1) as total_races,
  sum(calculated_points) AS total_points,
  avg(calculated_points) AS Avg_points
  from f1_presentation.calculated_race_results
  group by Team_name
  having count(1)>=100
  order by Avg_points desc

-- COMMAND ----------

select Team_name,
   count(1) as total_races,
  sum(calculated_points) AS total_points,
  avg(calculated_points) AS Avg_points
  from f1_presentation.calculated_race_results
  where race_year between 2011 and 2020
  group by Team_name
  having count(1)>=100
  order by Avg_points desc

-- COMMAND ----------

select Team_name,
   count(1) as total_races,
  sum(calculated_points) AS total_points,
  avg(calculated_points) AS Avg_points
  from f1_presentation.calculated_race_results
  where race_year between 2001 and 2011
  group by Team_name
  having count(1)>=100
  order by Avg_points desc
