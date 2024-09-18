# Databricks notebook source
dbutils.fs.ls("abfss://demo@formula1dl15.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl15.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl15.dfs.core.windows.net/circuits.csv"))
