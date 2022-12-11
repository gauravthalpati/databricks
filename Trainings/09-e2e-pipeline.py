# Databricks notebook source
# MAGIC %run ./00-my-creds

# COMMAND ----------

# Set the broker & topic name
topic="ktopic01"
broker="pkc-ldvr1.asia-southeast1.gcp.confluent.cloud:9092"


# COMMAND ----------

# MAGIC %md
# MAGIC # Spark Structured Streaming to write files to landing

# COMMAND ----------

df = (spark.readStream
    .format("kafka")
    .option("subscribe", "{}".format(topic))
    .option("kafka.bootstrap.servers", "{}".format(broker))
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret))
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "latest")
    .load()
    )

# COMMAND ----------

df2=df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

jsonSchema = StructType([
    StructField("ordertime",StringType(),True),
    StructField("orderid",StringType(),True),
    StructField("itemid",StringType(),True)
])


# COMMAND ----------

df3 = df2.withColumn("value", from_json(df2["value"], jsonSchema))



# COMMAND ----------

# Write the data using checkpointing for output file 
stream = df2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream.format("json") \
            .option("checkpointLocation", "/mnt/mydatastore/e2e/kafkacp") \
            .trigger (once = True) \
            .start("/mnt/mydatastore/e2e/autoldinp1") \
            .awaitTermination()


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Landing to bronze using Auto Loader

# COMMAND ----------

df=spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.inferColumnTypes", "true")  \
    .option("cloudFiles.schemaLocation", "/mnt/mydatastore/e2e/autoldschema")  \
    .load("/mnt/mydatastore/e2e/autoldinp1")

# COMMAND ----------

df2=df.writeStream.format("delta") \
   .trigger(once=True) \
   .option("checkpointLocation", "/mnt/mydatastore/e2e/autoldcp") \
   .start("/mnt/mydatastore/e2e/autoout") \
   .awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from delta.`/mnt/mydatastore/e2e/autoout/` order by key desc

# COMMAND ----------

# Write to another table
(df.writeStream.format("delta")
   .trigger(once=True)
   .option("checkpointLocation", "/mnt/mydatastore/e2e/autoldcp")
   .table("bronze_table_e2edemo"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_table_e2edemo;
