# Databricks notebook source
# MAGIC %sh
# MAGIC pip install git+https://github.com/rchynoweth/SnowflakeStreamReader.git@main

# COMMAND ----------

# MAGIC %sh pip freeze

# COMMAND ----------

from snowflake_cdc.snowflake_connect import SnowflakeConnect

# COMMAND ----------

from snowflake_cdc.snowflake_namespace import SnowflakeNamespace
from snowflake_cdc.snowflake_table import SnowflakeTable
from snowflake_cdc.snowflake_stream_reader import SnowflakeStreamReader

# COMMAND ----------

snowflake_creds = {
  'snowflake_user': dbutils.secrets.get(secret_scope, 'snowflake_user'),
  'snowflake_password': dbutils.secrets.get(secret_scope, 'snowflake_password'),
  'snowflake_account': snowflake_account
}
sfConnect = SnowflakeConnect(snowflake_creds)
sfConnect.run_query("select 1 ")

