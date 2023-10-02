# Databricks notebook source
# MAGIC %md
# MAGIC <img width="300px" src="https://docs.delta.io/latest/_static/delta-lake-logo.png">

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Vacuum Delta Tables
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM deltadbxfordba.customers

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/deltalake/[ADLS instance name]/customers/"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY deltadbxfordba.customers

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 1: Disable the default retention period by changing the spark configuration setting

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2: View which files will be removed when the `VACUUM` command is executed using `DRY RUN`

# COMMAND ----------

display(spark.sql("VACUUM deltadbxfordba.customers RETAIN 0 HOURS DRY RUN"))

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 3: Remove old files by executing the `VACUUM` command
# MAGIC
# MAGIC `VACUUM` command can be performed using Spark SQL or Delta Lake package commands

# COMMAND ----------

display(spark.sql("VACUUM deltadbxfordba.customers RETAIN 0 HOURS"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/deltalake/[ADLS instance name]/customers/"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY deltadbxfordba.customers
