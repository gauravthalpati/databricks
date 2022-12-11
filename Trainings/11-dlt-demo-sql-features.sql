-- Databricks notebook source
CREATE STREAMING LIVE TABLE bronze_sales
COMMENT "sales bronze table"
AS SELECT * FROM cloud_files("/mnt/mydatastore/dltdemo/dltinp/sales/", "csv");

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_customers
COMMENT "customers bronze table"
AS SELECT * FROM cloud_files("/mnt/mydatastore/dltdemo/dltinp/customers/", "csv");

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customer_cleaned(
  CONSTRAINT valid_customer EXPECT (cust_city <> 'Pune') ON VIOLATION DROP ROW
)
COMMENT "customers silver table"
AS
  SELECT *
  FROM STREAM(LIVE.bronze_customers) 
  

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sale_cleaned(
  CONSTRAINT valid_sale EXPECT (cust_id <> ' ') ON VIOLATION DROP ROW
)
COMMENT "customers silver table"
AS
  SELECT *
  FROM STREAM(LIVE.bronze_sales) 

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE target_gold(
  )
COMMENT "gold table"
AS
  SELECT s.cust_id,c.cust_name
  FROM STREAM(LIVE.sale_cleaned) s
  LEFT JOIN LIVE.customer_cleaned c
    ON c.cust_id = s.cust_id
 
