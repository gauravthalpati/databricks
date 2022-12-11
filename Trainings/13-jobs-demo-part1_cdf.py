# Databricks notebook source
df= spark.read.format("csv") \
    .options(inferSchema="True") \
    .options(header="True") \
    .load("/databricks-datasets/flights/departuredelays.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists dltsqldemodb.bronze_flights_cdf
# MAGIC (
# MAGIC date int,
# MAGIC delay int,
# MAGIC distance int,
# MAGIC origin string,
# MAGIC destination string
# MAGIC )
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

df.write.mode("append").saveAsTable("dltsqldemodb.bronze_flights_cdf")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dltsqldemodb.bronze_flights_cdf;
