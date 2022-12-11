-- Databricks notebook source
-- MAGIC %python
-- MAGIC print ("Hello cdf")

-- COMMAND ----------

CREATE table if not exists customer1 (id INT, name STRING, age INT) 
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

describe extended customer1;

-- COMMAND ----------

insert into table customer1 values 
(1,"Captain",500),(2,"Tony",38),(3,"Vision",77);

-- COMMAND ----------

select * from customer1;

-- COMMAND ----------

SELECT * FROM table_changes('customer1', 0, 10)

-- COMMAND ----------

SELECT * FROM table_changes('customer1', 2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### _change_type : insert, update_preimage , update_postimage, delete
-- MAGIC #### _commit_version : The Delta log or table version containing the change.
-- MAGIC #### _commit_timestamp : The timestamp associated when the commit was created.

-- COMMAND ----------

update customer1 set id=10 where id=1;

-- COMMAND ----------

select * from customer1;

-- COMMAND ----------

SELECT * FROM table_changes('customer1', 2)

-- COMMAND ----------

delete from customer where id=2;

-- COMMAND ----------

SELECT * FROM table_changes('customer', 4)
