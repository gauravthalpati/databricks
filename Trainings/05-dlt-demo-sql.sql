-- Databricks notebook source
CREATE STREAMING LIVE TABLE p_bronze
COMMENT "bronze table"
AS SELECT * FROM cloud_files("/mnt/mydatastore/dltpythondemo/input/", "json");

-- COMMAND ----------

CREATE STREAMING LIVE TABLE p_silver
COMMENT "silver table"
AS SELECT * FROM STREAM(LIVE.p_bronze)

-- COMMAND ----------

CREATE STREAMING LIVE TABLE p_gold
COMMENT "gold table"
AS SELECT * FROM STREAM(LIVE.p_silver)
where age > 30
