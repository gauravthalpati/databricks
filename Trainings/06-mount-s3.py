# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Configure Secrets in Scope

# COMMAND ----------

# databricks secrets create-scope --scope awskeys
# databricks secrets list-scopes

# databricks secrets put --scope awskeys --key akey
# databricks secrets put --scope awskeys --key asecret
# databricks secrets list --scope awskeys

# dbutils.secrets.get()

# COMMAND ----------

access_key = dbutils.secrets.get(scope = "awskeys", key = "akey")
secret_key = dbutils.secrets.get(scope = "awskeys", key = "asecret")
encoded_secret_key = secret_key.replace("/", "%2F")

# COMMAND ----------

display(access_key)
display(secret_key)

# COMMAND ----------

aws_bucket_name = "databricks-alldata-22"
mount_name = "mydatastore2"

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Mount the storage

# COMMAND ----------

dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")
display(dbutils.fs.ls(f"/mnt/{mount_name}"))

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/mydatastore2"))
