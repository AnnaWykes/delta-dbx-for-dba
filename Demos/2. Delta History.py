# Databricks notebook source
# MAGIC %md
# MAGIC <img width="300px" src="https://docs.delta.io/latest/_static/delta-lake-logo.png">

# COMMAND ----------

# MAGIC %md
# MAGIC ### History and Versions

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM deltadbxfordba.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE deltadbxfordba.customers -- Returns the table schema
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY deltadbxfordba.customers -- Displays the most recent changes made to the table (inserts, updates, optimize, vacuum etc)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM deltadbxfordba.customers
# MAGIC VERSION AS OF 22

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO deltadbxfordba.customers VALUES (
# MAGIC 9999,
# MAGIC "FALSE",
# MAGIC "Mr",
# MAGIC "John",
# MAGIC "Bob",
# MAGIC "Smith",
# MAGIC "Suffix",
# MAGIC "CompanyName",
# MAGIC "SalesPerson",
# MAGIC "orlando0@adventure-works.com",
# MAGIC "245-555-0173",
# MAGIC "L/Rlwxzp4w7RWmEgXX+/",
# MAGIC "A7cXaePEPcp+KwQhl2fJL7w=",
# MAGIC "3f5ae95e-b87d-4aed-95b4-c3797afcb74f",
# MAGIC "2005-08-01T00:00:00.000+0000"
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY deltadbxfordba.customers -- Displays the most recent changes made to the table (inserts, updates, optimize, vacuum etc)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM deltadbxfordba.customers
# MAGIC VERSION AS OF 41
