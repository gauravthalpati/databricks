# Databricks notebook source
# MAGIC %sh
# MAGIC ls -l /dbfs/mnt/mydatastore/dltpythondemo/input/

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls -l fbf6790d-5b30-4282-9e28-e66f30768b34

# COMMAND ----------

# MAGIC %sh
# MAGIC cp /dbfs/mnt/mydatastore/dltpythondemo/sample/people2.json /dbfs/mnt/mydatastore/dltpythondemo/input/

# COMMAND ----------

# MAGIC %sh
# MAGIC date

# COMMAND ----------

# MAGIC %sh
# MAGIC rm /dbfs/mnt/mydatastore/dltpythondemo/input/people3.json

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/mydatastore/dltpythondemo/sample/

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC cat /dbfs/mnt/mydatastore/dltpythondemo/input/people2.json

# COMMAND ----------

# MAGIC %sh
# MAGIC cp /dbfs/mnt/mydatastore/dltpythondemo/sample/people4.json /dbfs/mnt/mydatastore/dltpythondemo/input/

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC rm /dbfs/mnt/mydatastore/dltpythondemo/input/people3.json

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls -l /dbfs/mnt/mydatastore/dltpythondemo/input/

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/mydatastore/dltdemo/dltinp/

# COMMAND ----------

# MAGIC %sh
# MAGIC cp /dbfs/mnt/mydatastore/dltdemo/dltsample/customer* /dbfs/mnt/mydatastore/dltdemo/dltinp/customers/

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,to_date(date,'dd-MM-yyyy') as dd from sqlfeatures.bronze_sales where to_date(date,'dd-MM-yyyy') > 2022-01-01;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sqlfeatures.bronze_customers where cust_city <> 'Pune';

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table sqlfeatures.bronze_customers;

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls /dbfs/pipelines/fbf6790d-5b30-4282-9e28-e66f30768b34/tables/

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls -l /dbfs/pipelines/

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls -l /dbfs/mnt/mydatastore/dltdemo/

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sqlfeatures.bronze_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sqlfeatures.sale_cleaned;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sqlfeatures.customer_cleaned;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sqlfeatures.target_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dltpythondemodb.mypeople_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dltpythondemodb.mypeople_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dltsqldemodb.p_gold;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dltpythondemodb.mypeople_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dltpythondemodb.mypeople_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history dltpythondemodb.mypeople_bronze;

# COMMAND ----------

# MAGIC %sh 
# MAGIC 
# MAGIC cat  /dbfs/mnt/mydatastore/dltdemo/dltinp/sales/sales.csv

# COMMAND ----------

# MAGIC %sh 
# MAGIC 
# MAGIC cat /dbfs/mnt/mydatastore/dltdemo/dltinp/customers/customer.csv

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from sqlfeatures.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dltsqldemodb.gold_flights;
