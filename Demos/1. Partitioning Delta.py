# Databricks notebook source
# MAGIC %md
# MAGIC <img width="300px" src="https://docs.delta.io/latest/_static/delta-lake-logo.png">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partition Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC You will need to create a Databricks scope and populate 
# MAGIC - clientid (Service Principal)
# MAGIC - clientsecret (Service Principal)
# MAGIC - subscriptionid (Azure Subscription ID)
# MAGIC
# MAGIC All Data have come from Adventure Works customer table

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.sadeltadbxfordba.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.sadeltadbxfordba.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.sadeltadbxfordba.dfs.core.windows.net", dbutils.secrets.get("demo-scope", "clientid")) 
spark.conf.set("fs.azure.account.oauth2.client.secret.sadeltadbxfordba.dfs.core.windows.net", dbutils.secrets.get("demo-scope", "clientsecret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.sadeltadbxfordba.dfs.core.windows.net", f"https://login.microsoftonline.com/{dbutils.secrets.get('demo-scope', 'subscriptionid')}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1: Load the Data into Dataframe

# COMMAND ----------

sourcePath = "abfss://main@[ADLS instance name].dfs.core.windows.net/Raw/customers"
sourcePath

# COMMAND ----------

df = spark.read.format('parquet').load(sourcePath)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Create Delta table without `.partitionBy`

# COMMAND ----------

deltaPath_NoPart = 'abfss://main@[ADLS instance name].dfs.core.windows.net/delta/customers/NoPartition'

( df.write
   .format("delta")
   .mode("overwrite")
   .save("dbfs:/mnt/deltalake/[ADLS container name]/customers/") )

spark.sql("""
          CREATE TABLE deltadbxfordba.customers_nopart
          LOCATION '{0}'
          """.format(deltaPath_NoPart))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3: Explore the Delta files and metadata

# COMMAND ----------

# MAGIC %md
# MAGIC Query the Delta files to see the impact of previous queries, there are now lots of files 

# COMMAND ----------

display(dbutils.fs.ls(deltaPath_NoPart))

# COMMAND ----------

# MAGIC %md
# MAGIC Query the `deltadbxfordba.customers_nopart` metadata to see the impact of previous queries

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL deltadbxfordba.customers_nopart

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4: Create Delta table with `.partitionBy`

# COMMAND ----------

deltaPath_Partitioned = 'abfss://main@[ADLS instance name].dfs.core.windows.net/delta/customers/partitioned'

( df.write
   .format("delta")
   .mode("overwrite")
   .partitionBy("ModifiedDate")
   .save(deltaPath_Partitioned)
)

spark.sql("""
          CREATE TABLE deltadbxfordba.customers_partitioned 
          USING DELTA 
          LOCATION '{0}'
          """.format(deltaPath_Partitioned))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5: Explore the Delta files and metadata

# COMMAND ----------

# MAGIC %md
# MAGIC Query the Delta files to see the impact of previous queries, there are now lots of files 

# COMMAND ----------

deltaPath_Partitioned = deltaPath_Partitioned
display(dbutils.fs.ls(deltaPath_Partitioned))

# COMMAND ----------

# MAGIC %md
# MAGIC Query the `deltadbxfordba.customers_partitioned` metadata to see the impact of previous queries, there are now lots of small and inefficient files. 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL deltadbxfordba.customers_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimize partitioned Delta table
# MAGIC
# MAGIC You can apply `OPTIMIZE` methods against partitioned Delta tables to combine the small files into large files and decreased file sizes

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1: Optimize Delta table using `OPTIMIZE` command

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE deltadbxfordba.customers_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC Query the `deltadbxfordba.customers_partitioned` metadata to validate the changes, the number of files has now been compacted into a smaller files.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL deltadbxfordba.customers_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Optimize Delta table using `ZORDER` command
# MAGIC
# MAGIC
# MAGIC You cannot use the table partition column as ZORDER column

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE deltadbxfordba.customers_partitioned ZORDER BY (LastName)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up

# COMMAND ----------

# ## Clean Up
# ## Remove delta table
spark.sql("DROP TABLE IF EXISTS deltadbxfordba.customers_partitioned")
spark.sql("DROP TABLE IF EXISTS deltadbxfordba.customers_nopart")

# ## Removed File
# dbutils.fs.rm("dbfs:/mnt/deltalake/masteringdelta/nyctaxi/yellow_tripdata",True)


