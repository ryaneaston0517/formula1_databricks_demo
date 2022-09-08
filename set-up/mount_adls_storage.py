# Databricks notebook source
#unmount DL Storage
# dbutils.fs.unmount("/mnt/sadatabrickstutorial/raw")
# dbutils.fs.unmount("/mnt/sadatabrickstutorial/processed")

# COMMAND ----------

#walk through dbutils and dealing with secrets
dbutils.secrets.help()

# COMMAND ----------

#the secret scope that was created
dbutils.secrets.listScopes()

# COMMAND ----------

#see what is in the scope: 3 keys defined
dbutils.secrets.list('formula1-scope')

# COMMAND ----------

#get values
dbutils.secrets.get(scope="formula1-scope", key='databricks-app-client-id')

# COMMAND ----------

#Variable Setup
#from blob storage
storage_account_name = "sadatabrickstutorial"

#from service principal
client_id = dbutils.secrets.get(scope="formula1-scope", key='databricks-app-client-id')
tenant_id = dbutils.secrets.get(scope="formula1-scope", key='databricks-app-tenant-id')
client_secret = dbutils.secrets.get(scope="formula1-scope", key='databricks-app-client-secret')

#Credentials are exposed in clear text:  bad practice.  Use Secrets in Databricks and store in Key Vault.

# COMMAND ----------

#set up config parameters
#take in client id
#apply client secret
#to create endpoint we need the tenant id
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret": f"{client_secret}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

#mount raw storage - need to use the dbutils fs.mount.
def mount_adls(container_name):
    dbutils.fs.mount(source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
                    mount_point = f"/mnt/{storage_account_name}/{container_name}",
                    extra_configs = configs)

# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

#commant to mount storage has succeeded.
#verify:
dbutils.fs.ls(f"/mnt/{storage_account_name}")

# COMMAND ----------

#empty.  But exists
#list all mounts
dbutils.fs.mounts()

# COMMAND ----------


