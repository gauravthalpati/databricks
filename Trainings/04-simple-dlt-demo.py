# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

@dlt.create_table(
  comment="bronze table"
)
def mypeople_bronze():          
  return (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true") 
    .load("/mnt/mydatastore/dltpythondemo/input/")
  )

# COMMAND ----------

@dlt.table(
  comment="gold table"
)
def mypeople_gold():
  return (
    dlt.read_stream("mypeople_silver")
      .filter(expr("age > 50"))
       )

# COMMAND ----------

@dlt.table(
  comment="silver table"
)

def mypeople_silver():
  return (
    dlt.read_stream("mypeople_bronze")
   .select("age", "name")
  )
