-- Databricks notebook source

select * from dltsqldemodb.bronze_flights;

-- COMMAND ----------


create table if not exists dltsqldemodb.silver_flights
select * from dltsqldemodb.bronze_flights
where destination="ATL"

-- COMMAND ----------

select count(*) from dltsqldemodb.silver_flights;

-- COMMAND ----------

create table if not exists dltsqldemodb.gold_flights
(
origin string,
total_count int
)

-- COMMAND ----------

insert into dltsqldemodb.gold_flights
select origin,count(*) from dltsqldemodb.silver_flights
group by origin
having count(*) > 2000


-- COMMAND ----------

select * from  dltsqldemodb.gold_flights;
