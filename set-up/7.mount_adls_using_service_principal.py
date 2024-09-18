# Databricks notebook source
client_id = dbutils.secrets.get(scope = 'formula1-scope', key= 'formula1dl-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key= 'formula1dl-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key= 'formula1dl-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dl15.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl15/demo",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dl15/demo")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl15/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl15/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# dbutils.fs.unmount("/mnt/formula1dl15/demo")

# COMMAND ----------


