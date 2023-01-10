-- Databricks notebook source
-- Databricks notebook source
CREATE STREAMING LIVE TABLE p_bronze
COMMENT "bronze table"
AS SELECT * FROM cloud_files("/tmp/", "json");


-- COMMAND ----------

-- COMMAND ----------

CREATE STREAMING LIVE TABLE p_silver
COMMENT "silver table"
AS SELECT * FROM STREAM(LIVE.p_bronze)


-- COMMAND ----------

-- COMMAND ----------

CREATE STREAMING LIVE TABLE p_gold
COMMENT "gold table"
AS SELECT * FROM STREAM(LIVE.p_silver)
where age > 30

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Add Sample data as below from another non DLT notebook
-- MAGIC 
-- MAGIC ----------Round 1, Key = name, Sequence on ts
-- MAGIC %sh
-- MAGIC echo '{"name":"XYZ","age":10,"ts":"02"}' >> /dbfs/tmp/people1.json
-- MAGIC 
-- MAGIC %sh
-- MAGIC echo '{"name":"PQR","age":50,"ts":"02"}' >> /dbfs/tmp/people2.json
-- MAGIC 
-- MAGIC ----------Round 2 Key = name, Sequence on ts
-- MAGIC %sh
-- MAGIC echo '{"name":"XYZ","age":90,"ts":"03"}' >> /dbfs/tmp/people3.json

-- COMMAND ----------

-- SCD POC --

-- Create and populate the target table.
CREATE OR REFRESH STREAMING LIVE TABLE target;

APPLY CHANGES INTO
  live.target
FROM
  stream(live.p_silver)
KEYS
  (name)
SEQUENCE BY
  ts
STORED AS
  SCD TYPE 2;
