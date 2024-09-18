# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.formula1dl15.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl15.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl15.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl15.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl15.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


