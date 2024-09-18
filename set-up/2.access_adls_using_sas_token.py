# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.formula1dl15.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl15.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl15.dfs.core.windows.net", "sp=rl&st=2023-03-23T11:54:11Z&se=2023-03-23T19:54:11Z&spr=https&sv=2021-12-02&sr=c&sig=avmlTVD6ZqJInYSEQaLWj8gXZj6qsYZOhmIVb4Dg9tg%3D")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl15.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl15.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl15.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


