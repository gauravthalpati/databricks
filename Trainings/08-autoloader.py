# Databricks notebook source
# MAGIC %md
# MAGIC # Autoloader Demo

# COMMAND ----------

#https://docs.databricks.com/ingestion/auto-loader/index.html

# COMMAND ----------

display(dbutils.fs.ls("/mnt/mydatastore"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/mydatastore/autoloaddemo/sampledata"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/mydatastore/autoloaddemo/autoldinp"))

# COMMAND ----------

df=spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.inferColumnTypes", "true")  \
    .option("cloudFiles.schemaLocation", "/mnt/mydatastore/autoloaddemo/autoldschema")  \
    .load("/mnt/mydatastore/autoloaddemo/autoldinp")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df2=df.writeStream.format("delta") \
   .trigger(once=True) \
   .option("checkpointLocation", "/mnt/mydatastore/autoloaddemo/autoldcp") \
   .start("/mnt/mydatastore/autoloaddemo/autoout") \
   .awaitTermination()
   

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Query the delta file using SQL
# MAGIC select * from delta.`/mnt/mydatastore/autoloaddemo/autoout/`

# COMMAND ----------

# Write to table
(df.writeStream.format("delta")
   .trigger(once=True)
   .option("checkpointLocation", "/mnt/mydatastore/autoloaddemo/autoldtablecp")
   .table("table_autodemo2"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_autodemo2;
