# Databricks notebook source
# MAGIC %run ./00-my-creds

# COMMAND ----------

# Set the broker & topic name
topic="topic_source"
write_topic ="topic_target"
broker="<broker-server>:9092"
streamdataout1="bronze_stream_f1"


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

df2=df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# COMMAND ----------

display(df2)

# COMMAND ----------

# Write Stream to Kafka Topic, first create the topic in Confluent Kafka
ds=df \
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "{}".format(broker)) \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret)) \
  .option("kafka.ssl.endpoint.identification.algorithm", "https") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("checkpointLocation", "/nov2022/checkpoint/{}_{}".format(topic,streamdataout1)) \
  .trigger(processingTime ="30 seconds") \
  .option("topic", "{}".format(write_topic)) \
  .start()

    
