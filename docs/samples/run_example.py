# Databricks notebook source
# MAGIC %sh
# MAGIC pip install git+https://github.com/rchynoweth/SnowflakeStreamReader.git@main

# COMMAND ----------

from pyspark.sql.functions import col 
from snowflake_cdc.snowflake_connect import SnowflakeConnect
from snowflake_cdc.snowflake_namespace import SnowflakeNamespace
from snowflake_cdc.snowflake_table import SnowflakeTable
from snowflake_cdc.snowflake_stream_reader import SnowflakeStreamReader

# COMMAND ----------

dbutils.widgets.text("secretScope", "")
dbutils.widgets.text("snowflakeAccount", "") # https://<snowflake_account>.snowflakecomputing.com/ 
dbutils.widgets.text("snowflakeDatabase", "")
dbutils.widgets.text("snowflakeSchema", "")
dbutils.widgets.text("snowflakeStage", "")
dbutils.widgets.text("adlsLocation","")
dbutils.widgets.text("databricksSchema", "")
dbutils.widgets.text("fileFormatName", "")
dbutils.widgets.text("stagePath", "")

# COMMAND ----------

secret_scope = dbutils.widgets.get("secretScope")
snowflake_account = dbutils.widgets.get("snowflakeAccount")
snowflake_database = dbutils.widgets.get("snowflakeDatabase")
snowflake_schema = dbutils.widgets.get("snowflakeSchema")
snowflake_stage = dbutils.widgets.get("snowflakeStage")
databricks_schema = dbutils.widgets.get("databricksSchema")
adls_location = dbutils.widgets.get("adlsLocation")
file_format_name = dbutils.widgets.get('fileFormatName')
stage_path = dbutils.widgets.get('stagePath')

sas_token = dbutils.secrets.get(secret_scope, "snowflakeSasToken")

container_name = adls_location.split("/")[2].split("@")[0]
storage_account_name = adls_location.split("/")[2].split("@")[1].replace(".dfs.core.windows.net", "")

# COMMAND ----------

# DBTITLE 1,Test Connection to Snowflake 
snowflake_creds = {
  'snowflake_user': dbutils.secrets.get(secret_scope, 'snowflake_user'),
  'snowflake_password': dbutils.secrets.get(secret_scope, 'snowflake_password'),
  'snowflake_account': snowflake_account
}
sfConnect = SnowflakeConnect(snowflake_creds)
sfConnect.run_query("select 1 ")


# COMMAND ----------

# DBTITLE 1,Set Table Configurations
config = {
 'file_format_name':file_format_name, 
 'sas_token':sas_token,
 'stage_name':snowflake_stage,
 'storage_account_name':storage_account_name,
 'container_name':container_name,
 'snowflake_database':snowflake_database,
 'snowflake_schema':snowflake_schema, 
 'additional_path':stage_path,
 'database_name':"demo", 
 'schema_name':"rac_schema",
 'table_name':"test_supplier", 
 'merge_keys':['s_suppkey'], 
 'snowflake_user': dbutils.secrets.get(secret_scope, 'snowflake_user'),
 'snowflake_password': dbutils.secrets.get(secret_scope, 'snowflake_password'),
 'snowflake_account': snowflake_account
}

# COMMAND ----------

snowflakeStreamer = SnowflakeStreamReader(spark, dbutils)

# COMMAND ----------

streamDF = snowflakeStreamer.read_snowflake_stream(config)

# COMMAND ----------

display(streamDF.filter(col("s_suppkey") == 1))

# COMMAND ----------


