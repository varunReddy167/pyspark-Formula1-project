-- Databricks notebook source
use f1_presentation

-- COMMAND ----------

desc driver_standings

-- COMMAND ----------

create  or replace Temp VIEW v_driver_standings_2018
as
SELECT  race_year,driver_name,team,total_points,wins,rank
 from driver_standings
 where race_year= 2018

-- COMMAND ----------

select * from v_driver_standings_2018;

-- COMMAND ----------

create  or replace Temp VIEW v_driver_standings_2020
as
SELECT  race_year,driver_name,team,total_points,wins,rank
 from driver_standings
 where race_year= 2020

-- COMMAND ----------

select * from v_driver_standings_2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####inner join
-- MAGIC

-- COMMAND ----------

select *
from v_driver_standings_2018 d_2018
join v_driver_standings_2020 d_2020
on (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####LEFT join

-- COMMAND ----------

select *
from v_driver_standings_2018 d_2018
left join v_driver_standings_2020 d_2020
on (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####right join

-- COMMAND ----------

select *
from v_driver_standings_2018 d_2018
right join v_driver_standings_2020 d_2020
on (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Full join

-- COMMAND ----------

select *
from v_driver_standings_2018 d_2018
full join v_driver_standings_2020 d_2020
on (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####semi join

-- COMMAND ----------

select *
from v_driver_standings_2018 d_2018
semi join v_driver_standings_2020 d_2020
on (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Anti join

-- COMMAND ----------

select *
from v_driver_standings_2018 d_2018
Anti join v_driver_standings_2020 d_2020
on (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####cross join

-- COMMAND ----------

select *
from v_driver_standings_2018 d_2018
cross join v_driver_standings_2020 d_2020

