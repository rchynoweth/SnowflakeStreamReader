# Databricks notebook source
# MAGIC %md
# MAGIC ## Example Set Up and Configuration
# MAGIC 
# MAGIC 
# MAGIC The configuration is pretty standard for a widget based approach. If you are using a json config file or if you are using environment variables then it may not be required. 
# MAGIC 
# MAGIC 
# MAGIC **Example Notebook Widgets**: 
# MAGIC - `adlsLocation`: this is the directory that the data will be written to. This is used in the Snowflake Stage definition and can be used to read the files using Auto Loader. 
# MAGIC   - This value should only include the container i.e. `abfss://container_name@storage_account_name.dfs.core.windows.net`. 
# MAGIC   - If you wish to include directory paths then it should be provided in the `stage_path` widget.  
# MAGIC - `databricksSchema`: the databricks schema to save data to as tables 
# MAGIC - `secretScope`: the name of the secret scope to be used 
# MAGIC - `snowflakeAccount`: the snowflake account to use  
# MAGIC   - If your URL is `https://abc123.snowflakecomputing.com/` then the value is `abc123`  
# MAGIC   - If your URL is `https://app.snowflake.com/us-west-2/abc123/` then the value is `abc123`  
# MAGIC - `snowflakeDatabase`: Snowflake database to use   
# MAGIC - `snowflakeSchema`: Snowflake schema to use   
# MAGIC - `stagePath`: the additional path that you want to use for your Snowflake stage. If not provided then the stage will unload data to the base directory of the storage container.  
# MAGIC   - example: `stagePath = '/my/dir'` and you have a table `my_database.my_schema.my_table` then the data will be available at the following location: `abfss://container_name@storage_account_name.dfs.core.windows.net/my/dir/my_database/my_schema/my_table/year=yyyy/month=mm/day=dd`   
# MAGIC   - example: `stagePath = ''` and you have a table `my_database.my_schema.my_table` then the data will be available at the following location: `abfss://container_name@storage_account_name.dfs.core.windows.net/my_database/my_schema/my_table/year=yyyy/month=mm/day=dd`   
# MAGIC - `fileFormatName`: name of the file format you want to create.  
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Required Secrets**:  
# MAGIC - snowflakeSasToken: the sas token that is used to authenticate against ADLS from Snowflake   
# MAGIC - snowflake_user: the username to connect to snowflake   
# MAGIC - snowflake_password: the password to connect to snowflake   

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install snowflake-connector-python

# COMMAND ----------

# MAGIC %pip install 'typing-extensions>=4.3.0'

# COMMAND ----------

from libs.snowflake_connect import SnowflakeConnect
from libs.snowflake_namespace import SnowflakeNamespace
from libs.snowflake_table import SnowflakeTable
from libs.databricks_snowflake_reader import DatabricksSnowflakeReader
from time import sleep
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Set Widgets
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

# DBTITLE 1,Set Variables
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

reset = False 
if reset:
  dbutils.fs.rm(adls_location+"/"+stage_path)

# COMMAND ----------

# DBTITLE 1,Create and use schema 
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {databricks_schema}")
spark.sql(f"USE {databricks_schema}")

# COMMAND ----------

# DBTITLE 1,Create a reader object to read the Snowflake CDC data 
snowflakeStreamer = DatabricksSnowflakeReader(spark)

# COMMAND ----------

# DBTITLE 1,Set Snowflake Credentials and Test Query Connection
snowflake_creds = {
  'user': dbutils.secrets.get(secret_scope, 'snowflake_user'),
  'password': dbutils.secrets.get(secret_scope, 'snowflake_password'),
  'snowflake_account': snowflake_account
}
sfConnect = SnowflakeConnect(snowflake_creds)
sfConnect.run_query("select 1 ")

# COMMAND ----------

# DBTITLE 1,Create database object to manage cdc unloads
namespace_config = {
 'file_format_name':file_format_name, 
 'sas_token':sas_token,
 'stage_name':snowflake_stage,
 'storage_account_name':storage_account_name,
 'container_name':container_name,
 'snowflake_database':snowflake_database,
 'snowflake_schema':snowflake_schema, 
 'additional_path':stage_path
}
sfNamespace = SnowflakeNamespace(namespace_config)

# COMMAND ----------

# DBTITLE 1,Add tables to database object 
t1 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_customer", merge_keys=['c_custkey'])
t2 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_lineitem", merge_keys=['l_orderkey', 'l_linenumber'])
t3 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_nation", merge_keys=['n_nationkey'])
t4 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_orders", merge_keys=['o_orderkey'])
t5 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_part", merge_keys=['p_partkey'])
t6 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_partsupp", merge_keys=['ps_partkey', 'ps_suppkey'])
t7 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_region", merge_keys=['r_regionkey'])
t8 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_supplier", merge_keys=['s_suppkey'])



sfNamespace.add_table(t1)
sfNamespace.add_table(t2)
sfNamespace.add_table(t3)
sfNamespace.add_table(t4)
sfNamespace.add_table(t5)
sfNamespace.add_table(t6)
sfNamespace.add_table(t7)
sfNamespace.add_table(t8)

# COMMAND ----------

# DBTITLE 1,Once per database object - set up file format and stage 
file_query_id, stage_query_id = sfConnect.account_setup(sfNamespace)

# COMMAND ----------

# DBTITLE 1,For each table create a stream and a task
# a for loop should suffice as I imagine that table counts will be in the thousands at most for people which won't take long to execute that many queries 
for t in sfNamespace.tables:
  stream_query_id, task_query_id = sfConnect.table_setup(sfNamespace.tables.get(t), sfNamespace)

# COMMAND ----------

if reset:
  sleep(160) # snowflake task with a 1 minute schedule will take 1 minute to publish the first file

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example - Read Stage Files as Auto Loader Stream

# COMMAND ----------

# MAGIC %md
# MAGIC ### Append Only CDC Tables 

# COMMAND ----------

checkpoint_path = snowflakeStreamer.get_table_checkpoint_location(t1, sfNamespace)
schema_path = snowflakeStreamer.get_table_schema_location(t1, sfNamespace)
data_path = snowflakeStreamer.get_data_path(t1, sfNamespace)
print(checkpoint_path)
print(schema_path)
print(data_path)

# COMMAND ----------

dbutils.fs.rm(checkpoint_path, True)
dbutils.fs.rm(schema_path, True)
spark.sql(f"DROP TABLE IF EXISTS {t1.table_name}")

# COMMAND ----------

append_df = snowflakeStreamer.read_append_only_stream(data_path, schema_path)
display(append_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge CDC Tables - replicates tables in Snowflake

# COMMAND ----------

checkpoint_path = snowflakeStreamer.get_table_checkpoint_location(t1, sfNamespace)
schema_path = snowflakeStreamer.get_table_schema_location(t1, sfNamespace)
data_path = snowflakeStreamer.get_data_path(t1, sfNamespace)

# COMMAND ----------

dbutils.fs.rm(checkpoint_path, True)
dbutils.fs.rm(schema_path, True)
spark.sql(f"DROP TABLE IF EXISTS {t1.table_name}")

# COMMAND ----------

merge_df = snowflakeStreamer.read_merge_stream(dir_location=data_path, schema_path=schema_path, merge_keys=t1.get_merge_keys_as_string())

# COMMAND ----------

(merge_df.writeStream
 .option("checkpointLocation", checkpoint_path)
 .foreachBatch(snowflakeStreamer.write_merge_stream)
 .start() 
)

# COMMAND ----------

if reset:
  sleep(10)

# COMMAND ----------

display(spark.sql(f"select * from {t1.table_name} where c_custkey >= 150000"))

# COMMAND ----------


