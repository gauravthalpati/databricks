# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/2022/input/people2.json"
file_type = "json"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "dltdemodb.people_landing_json"

df.write.format("json").saveAsTable(permanent_table_name)

# COMMAND ----------

dbutils.fs.cp("2022/input/people.json", "/FileStore/mydata/people.json")


# COMMAND ----------

# Download using link https://<db-instance>.cloud.databricks.com/files/mydata/

# COMMAND ----------

dbutils.fs.cp("/FileStore/tables/people2.json","2022/input/people2.json")

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /dbfs/nov2022/input/

# COMMAND ----------

# MAGIC %sh
# MAGIC rm /dbfs/nov2022/input/people2.json
