# Databricks notebook source
# for mount in dbutils.fs.mounts():
#     print(mount.mountPoint)

# COMMAND ----------

def mount_adls(storage_account,container_name):
    # Get secrets
    client_id = dbutils.secrets.get(scope = 'formula1-scope', key= 'formula1dl-client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key= 'formula1dl-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula1-scope', key= 'formula1dl-client-secret')
    
    #Set spark config
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    #Unmount if it is already there
    if any(mount.mountPoint == f"/mnt/{storage_account}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account}/{container_name}")
    
    #Mount the storage container    
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account}/{container_name}",
      extra_configs = configs)

# COMMAND ----------

mount_adls('formula1dl15','raw')

# COMMAND ----------

mount_adls('formula1dl15','processed')

# COMMAND ----------

mount_adls('formula1dl15','presentation')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


