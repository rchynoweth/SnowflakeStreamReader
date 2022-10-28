# Databricks notebook source
# MAGIC %sh
# MAGIC pip install git+https://github.com/rchynoweth/SnowflakeStreamReader.git@main

# COMMAND ----------

from libs.snowflake_connect import SnowflakeConnect
from libs.snowflake_namespace import SnowflakeNamespace
from libs.snowflake_table import SnowflakeTable
from libs.snowflake_stream_reader import SnowflakeStreamReader

# COMMAND ----------

snowflake_creds = {
  'snowflake_user': dbutils.secrets.get(secret_scope, 'snowflake_user'),
  'snowflake_password': dbutils.secrets.get(secret_scope, 'snowflake_password'),
  'snowflake_account': snowflake_account
}
sfConnect = SnowflakeConnect(snowflake_creds)
sfConnect.run_query("select 1 ")

