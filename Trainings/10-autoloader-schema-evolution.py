# Databricks notebook source
# MAGIC %md
# MAGIC # Autoloader Demo

# COMMAND ----------

display(dbutils.fs.ls("/mnt/mydatastore/autoloadschemademo/inp"))

# COMMAND ----------

df=spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.inferColumnTypes", "true")  \
    .option("cloudFiles.schemaLocation", "/mnt/mydatastore/autoloadschemademo/cp")  \
    .option("cloudFiles.schemaEvolutionMode","rescue") \
    .load("/mnt/mydatastore/autoloadschemademo/inp")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df2=df.writeStream.format("delta") \
   .trigger(once=True) \
   .option("checkpointLocation", "/mnt/mydatastore/autoloadschemademo/cp") \
   .start("/mnt/mydatastore/autoloadschemademo/out") \
   .awaitTermination()
   

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/mnt/mydatastore/autoloadschemademo/out`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table gold.autold_schema
# MAGIC location "/mnt/mydatastore/autoloadschemademo/out"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.autold_schema where _rescued_data is not null;
