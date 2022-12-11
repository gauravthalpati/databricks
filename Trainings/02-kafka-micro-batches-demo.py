# Databricks notebook source
# MAGIC %run ./00-my-creds

# COMMAND ----------

# Set the broker & topic name
topic="ktopic01"
broker="pkc-ldvr1.asia-southeast1.gcp.confluent.cloud:9092"
dataout1="bronze_f13"
dataout2="bronze_f14"

# COMMAND ----------

# Read Stream from Kafka

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

# Cast Key & Value
df2=df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# COMMAND ----------

# Display new dataframe
display(df2)

# COMMAND ----------

#df3=df2.select("value") 

# COMMAND ----------

# Write the data using checkpointing for output file 
stream = df2.writeStream.format("delta") \
            .option("checkpointLocation", "/nov2022/checkpoint/{}_{}".format(topic,dataout1)) \
            .trigger (once = True) \
            .start("/nov2022/output/{}_{}".format(topic,dataout1)) \
            .awaitTermination()


# COMMAND ----------

# {}_{}".format(topic,dataout1)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls  /nov2022/checkpoint/ktopic01_bronze_f13/

# COMMAND ----------

display(dbutils.fs.ls("/nov2022/output/{}_{}".format(topic,dataout1)))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Query the delta file using SQL
# MAGIC select * from delta.`dbfs:/nov2022/output/ktopic01_bronze_f13`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create delta table from the file
# MAGIC 
# MAGIC create table if not exists bronzet1
# MAGIC location 'dbfs:/nov2022/output/ktopic01_bronze_f1'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Select records from table
# MAGIC select * from bronzet1

# COMMAND ----------

dataout2="bronze_f11"

# COMMAND ----------

# Write the data using checkpointing for output file 
stream = df2.writeStream.format("delta") \
            .option("checkpointLocation", "/nov2022/checkpoint/{}_{}".format(topic,dataout2)) \
            .trigger (once = True) \
            .start("/nov2022/output/{}_{}".format(topic,dataout2)) \
            .awaitTermination()


# COMMAND ----------

display(dbutils.fs.ls("/nov2022/output/{}_{}".format(topic,dataout2)))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Query the delta file using SQL
# MAGIC select * from delta.`dbfs:/nov2022/output/ktopic01_bronze_f11`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create delta table from the file
# MAGIC 
# MAGIC create table if not exists bronzet11
# MAGIC location 'dbfs:/nov2022/output/ktopic01_bronze_f11'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from bronzet11;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table bronzet1;
# MAGIC drop table bronzet2;
