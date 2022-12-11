-- Databricks notebook source
create table if not exists dltsqldemodb.silver_flights_cdf
(
date int,
delay int,
distance int,
origin string,
destination string
)
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------


insert into dltsqldemodb.silver_flights_cdf
select date,delay,distance,origin,destination from table_changes('dltsqldemodb.bronze_flights_cdf', 2)
where destination="ATL"

-- COMMAND ----------

select count(*) from dltsqldemodb.silver_flights_cdf;

-- COMMAND ----------

create table if not exists dltsqldemodb.gold_flights_cdf
(
origin string,
total_count int
)

-- COMMAND ----------

insert into dltsqldemodb.gold_flights_cdf
select origin,count(*) from table_changes('dltsqldemodb.silver_flights_cdf', 1)
group by origin
having count(*) > 2000


-- COMMAND ----------

select * from  dltsqldemodb.gold_flights_cdf;
