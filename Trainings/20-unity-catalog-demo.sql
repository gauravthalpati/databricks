-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS demo_catalog1;

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

USE CATALOG demo_catalog1;

-- COMMAND ----------

SHOW GRANT ON CATALOG demo_catalog1;

-- COMMAND ----------

-- Assign permission to admin or other users/groups
GRANT CREATE SCHEMA, CREATE TABLE, CREATE VIEW, USE CATALOG
ON CATALOG demo_catalog1
TO `account users`;

-- COMMAND ----------

SHOW GRANT ON CATALOG demo_catalog1;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS demo_schema1
COMMENT "Demo Schema";

-- COMMAND ----------


SHOW SCHEMAS;

-- COMMAND ----------

SHOW TABLES IN information_schema;

-- COMMAND ----------

select * from information_schema.tables;

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED demo_schema1;

-- COMMAND ----------

USE demo_schema1;

-- COMMAND ----------

SHOW EXTERNAL LOCATIONS;

-- COMMAND ----------

DESCRIBE EXTERNAL LOCATION demounity;

-- COMMAND ----------

SHOW STORAGE CREDENTIALS;

-- COMMAND ----------

DESCRIBE STORAGE CREDENTIAL `unity-cred`;

-- COMMAND ----------

SHOW GRANTS `account users` ON STORAGE CREDENTIAL `unity-cred`;


-- COMMAND ----------

SHOW GRANTS ON EXTERNAL LOCATION demounity;

-- COMMAND ----------

USE CATALOG demo_catalog1;
USE demo_schema1;
show tables;

-- COMMAND ----------

-- Working Solution ---
CREATE TABLE if not exists demo_table1
  (role_id Int, role_name String) 
  location "s3://<s3-bucket-name>/demodata/" 
  

 

-- COMMAND ----------

describe extended demo_table1;

-- COMMAND ----------

INSERT INTO TABLE demo_table1
VALUES
  (3, "Bruce"),
  (5, "Peter");

-- COMMAND ----------

SHOW TABLES IN demo_schema1;

-- COMMAND ----------

SELECT
  *
FROM
  demo_catalog1.demo_schema1.demo_table1;

-- COMMAND ----------

USE CATALOG demo_catalog1;
USE demo_schema1;
SELECT *
FROM demo_table1;
