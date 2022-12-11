# Databricks notebook source
display(dbutils.fs.ls("/databricks-datasets/"))

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC head -5 /dbfs/databricks-datasets/flights/departuredelays.csv

# COMMAND ----------

df= spark.read.format("csv") \
    .options(inferSchema="True") \
    .options(header="True") \
    .load("/databricks-datasets/flights/departuredelays.csv")

# COMMAND ----------

df.show(4)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("dltsqldemodb.bronze_flights")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dltsqldemodb.bronze_flights;
