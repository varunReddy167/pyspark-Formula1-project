-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style ="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers </h1?"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace TEMP VIEW v_dominant_drivers
AS
select driver_name,
   count(1) as total_races,
  sum(calculated_points) AS total_points,
  avg(calculated_points) AS Avg_points,
  rank() OVER(ORDER BY avg(calculated_points) desc) driver_rank
  from f1_presentation.calculated_race_results
  group by driver_name
  having count(1)>=50
  order by Avg_points desc

-- COMMAND ----------

select race_year, driver_name,
   count(1) as total_races,
  sum(calculated_points) AS total_points,
  avg(calculated_points) AS Avg_points
  from f1_presentation.calculated_race_results
  WHERE driver_name in (select driver_name from v_dominant_drivers where driver_rank <=10)
  group by race_year,driver_name
  order by race_year, Avg_points desc

-- COMMAND ----------

select race_year, driver_name,
   count(1) as total_races,
  sum(calculated_points) AS total_points,
  avg(calculated_points) AS Avg_points
  from f1_presentation.calculated_race_results
  WHERE driver_name in (select driver_name from v_dominant_drivers where driver_rank <=10)
  group by race_year,driver_name
  order by race_year, Avg_points desc

-- COMMAND ----------

select race_year, driver_name,
   count(1) as total_races,
  sum(calculated_points) AS total_points,
  avg(calculated_points) AS Avg_points
  from f1_presentation.calculated_race_results
  WHERE driver_name in (select driver_name from v_dominant_drivers where driver_rank <=10)
  group by race_year,driver_name
  order by race_year, Avg_points desc
