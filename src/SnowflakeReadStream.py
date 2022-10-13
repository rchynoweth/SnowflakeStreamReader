# Databricks notebook source
# MAGIC %md
# MAGIC # Reading CDC Changes from Snowflake  
# MAGIC 
# MAGIC **THIS NOTEBOOK ONLY SUPPORTS AZURE DATA LAKE GEN2 AT THIS TIME (10/12/2022)**
# MAGIC 
# MAGIC The Snowflake Spark connector is a great option for reading from and writing data to Snowflake, however, for larger data sizes it can be a bottle. It is recommended that for larger data sizes that users instead initial copy to an external stage from Snowflake as files, then use Databricks Auto Loader to read the staged files. This process helps automate this process by setting up CDC streams. If datasets are smaller then full table copies could be more simple.  
# MAGIC 
# MAGIC The goal of this notebook is to create a "stream" of CDC changes out of Snowflake to cloud storage. It will provide at most two tables:
# MAGIC 1. Append only Delta table of CDC changes 
# MAGIC     - Please note that we add `load_datetime` to the published files so that we can drop duplicate changes across files  
# MAGIC 1. Delta table that maintains current state using merge keys  
# MAGIC 
# MAGIC 
# MAGIC <br></br>
# MAGIC ## Required Setup in Snowflake  
# MAGIC In order to "stream" data efficiently out of Snowflake we will need to leverage some of Snowflake's CDC capabilities. This requires creating objects in Snowflake that unload data as files to cloud storage. Once in cloud storage we (Databricks) can use Auto Loader to read the files and write them to Delta. Auto Loader can be used as a DLT Pipeline (which I will **not** be showing here) or the Structured Streaming APIs (which I **will** be showing in this notebook).   
# MAGIC 
# MAGIC Assuming that you are implementing this process for many tables, this notebook can be used to complete the following **one** time:   
# MAGIC - Create a Snowflake `FILE FORMAT` of type `JSON`   
# MAGIC - Create a Snowflake `STAGE` using the `FILE FORMAT` and a Azure Storage SAS token 
# MAGIC 
# MAGIC For **each table** you want to load this notebook can be used to complete the following:  
# MAGIC - Create a Snowflake `STREAM` for your table (`<table_name>_stream`)    
# MAGIC - Create a Snowflake `TASK` to copy CDC data to ADLS Gen2 as json files  
# MAGIC 
# MAGIC **Required Widgets**: 
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
# MAGIC 
# MAGIC 
# MAGIC <br></br>
# MAGIC ## Snowflake SQL Example Code 
# MAGIC ```sql
# MAGIC ---- SET UP FOR ALL TABLES ----
# MAGIC 
# MAGIC -- Create Database
# MAGIC CREATE DATABASE IF NOT EXISTS <NAME OF YOUR SNOWFLAKE DATABASE>;
# MAGIC USE DATABASE <NAME OF YOUR SNOWFLAKE DATABASE>;
# MAGIC -- Create Schema 
# MAGIC CREATE SCHEMA IF NOT EXISTS <NAME OF YOUR SNOWFLAKE SCHEMA>;
# MAGIC USE SCHEMA <NAME OF YOUR SNOWFLAKE SCHEMA>;
# MAGIC 
# MAGIC -- Create a json file format to unload data 
# MAGIC CREATE OR REPLACE FILE FORMAT <NAME OF YOUR FILE FORMAT> 
# MAGIC TYPE = JSON;
# MAGIC 
# MAGIC -- Create an external stage - this is where I will unload data 
# MAGIC CREATE OR REPLACE STAGE <NAME OF YOUR STAGE>
# MAGIC URL = 'azure://<account>.blob.core.windows.net/<container>'
# MAGIC CREDENTIALS = (AZURE_SAS_TOKEN = '<SAS TOKEN>')
# MAGIC FILE_FORMAT = <NAME OF YOUR FILE FORMAT>
# MAGIC ;
# MAGIC 
# MAGIC 
# MAGIC ---- SET UP FOR EACH TABLE ---- 
# MAGIC 
# MAGIC -- Create a snowflake stream
# MAGIC CREATE OR REPLACE STREAM <NAME OF YOUR STREAM>
# MAGIC ON TABLE <NAME OF YOUR TABLE> 
# MAGIC APPEND_ONLY = FALSE -- gives updates and deletes
# MAGIC SHOW_INITIAL_ROWS = TRUE ; -- for the initial rows for the first pull then only new/updated rows 
# MAGIC 
# MAGIC -- Create a task that runs every minute 
# MAGIC CREATE OR REPLACE TASK <NAME OF YOUR TASK> 
# MAGIC SCHEDULE = '1 MINUTE' -- Change as needed 
# MAGIC ALLOW_OVERLAPPING_EXECUTION = FALSE -- if they overlap then we may get duplicates from the stream if the previous DML is not complete 
# MAGIC USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' -- using Snowflake Serverless compute 
# MAGIC AS 
# MAGIC (
# MAGIC   COPY INTO @<NAME OF YOUR STAGE>/<STORAGE DIR>/
# MAGIC   FROM (
# MAGIC       SELECT OBJECT_CONSTRUCT(*) as row_value FROM (SELECT *, current_timestamp() as load_datetime FROM {table_name}_stream )
# MAGIC       )
# MAGIC   include_query_id=true;  -- Ensures that each file we write has a unique name which is required for auto loader  
# MAGIC )
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC <br></br>
# MAGIC ## Items to Note  
# MAGIC - This process is set at the schema level, which means that you will need to have different stages and file formats for each schema you are accessing. 
# MAGIC - By default streams will be named `<table_name>_stream`
# MAGIC - By default tasks will be named `<table_name>_stream_task`
# MAGIC - Data Location in Storage (ADLS Gen2):
# MAGIC   - Input Parameters:
# MAGIC     - 
# MAGIC - Frequency of data loads:  
# MAGIC   - If you are unloading data out of Snowflake frequently (less than 10 minutes) it is likely best to run your Auto Loader stream 24/7 or use DLT. 
# MAGIC   - Less frequent unloading of data from Snowflake can likely be scheduled using `.trigger(once=True)`. If you do this you will want to try and align this with your Snowflake unload but it will be difficult to perfectly time it. For example, if data is loaded every 30 minutes (1:00, 1:30, ... 3:30,...) then maybe you want to schedule the Databricks job to run five minutes after (1:05, 1:35,...)  
# MAGIC - This is not a streaming solution and should not be advised as "good" architecture. This is help alliviate the pain of trying to load data out of Snowflake in a scalable and repeatable fashion. 
# MAGIC - Not all transactions will be reported. If multiple updates occur on a single row in between executions then only the latest update will be reported. This is due to the nature of Snowflake Streams. 
# MAGIC   - If this is unacceptable then users can use a `changes` clause which will allow users to manually implement CDC operations on Snowflake tables, but please note that the offsets are not managed by Snowflake. 
# MAGIC - This method reduces the total cost of reading data from Snowflake so that it can be used by other tools like Databricks.  
# MAGIC   - Spark connector acquires a double spend (Databricks and Snowflake) and this method is only Snowflake 
# MAGIC   - Other methods may require each user to read the data (i.e. a team of 10 people are reading the same base tables in Snowflake) which means that not only are individuals re-reading data but the entire team is duplicating this effort. Getting data out of Snowflake and into ADLS reduces the number of reads on a table to 1. 
# MAGIC   
# MAGIC   
# MAGIC   
# MAGIC ## Using a Configuration File   
# MAGIC 
# MAGIC Using this framework, engineers can easily create namespace and table objects. The namespace object (`sfNamespace`) can have a collection of table objects (`SnowflakeTable`). To streamline and make this a more simple process, users can provide a json configuration file that looks like the example below. You will notice that many of the parameters to the database object are provided via widgets in this notebook.   
# MAGIC ```json
# MAGIC {
# MAGIC     "snowflake_database":"my_database",
# MAGIC     "snowflake_schema": "my_schema",
# MAGIC     "stage_name": "my_snowflake_stage_name",
# MAGIC     "storage_account_name": "myadlsgen2", 
# MAGIC     "container_name": "mystoragecontainer",
# MAGIC     "additional_path": "/my/dir/in/adls",
# MAGIC     "sas_token": "storage_sas_token",
# MAGIC     "file_format_name": "my_snowflake_file_format_name",
# MAGIC     "file_format_type": "json",
# MAGIC     "tables": [
# MAGIC         {
# MAGIC             "database_name":"my_database",
# MAGIC             "schema_name":"my_schema",
# MAGIC             "table_name": "table_name",
# MAGIC             "merge_keys": ["id"],
# MAGIC             "task_schedule": "1 MINUTE",
# MAGIC             "task_warehouse_size": "XSMALL"
# MAGIC         },
# MAGIC         {
# MAGIC             "database_name":"my_database",
# MAGIC             "schema_name":"my_schema",
# MAGIC             "name": "table_name2",
# MAGIC             "merge_keys": ["id"],
# MAGIC             "task_schedule": "1 MINUTE",
# MAGIC             "task_warehouse_size": "SMALL"
# MAGIC         },
# MAGIC         {
# MAGIC             "database_name":"my_database",
# MAGIC             "schema_name":"my_schema",
# MAGIC             "name": "table_name3",
# MAGIC             "merge_keys": ["id"],
# MAGIC             "task_schedule": "5 MINUTE",
# MAGIC             "task_warehouse_size": "XSMALL"
# MAGIC         }
# MAGIC     ]
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install snowflake-connector-python

# COMMAND ----------

from libs.snowflake_connect import SnowflakeConnect
from libs.snowflake_namespace import SnowflakeNamespace
from libs.snowflake_table import SnowflakeTable
from libs.databricks_snowflake_reader import DatabricksSnowflakeReader
from pyspark.sql.types import *
from pyspark.sql.functions import *

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

spark.sql(f"USE {databricks_schema}")

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
sfNamespace.add_table(SnowflakeTable(name="test_stream_table", merge_keys=['id']))

# COMMAND ----------

# DBTITLE 1,Once per database object - set up file format and stage 
file_query_id, stage_query_id = sfConnect.account_setup(sfNamespace)

# COMMAND ----------

# DBTITLE 1,For each table create a stream and a task
# a for loop should suffice as I imagine that table counts will be in the thousands at most for people which won't take long to execute that many queries 
for t in sfNamespace.tables:
  stream_query_id, task_query_id = sfConnect.table_setup(sfNamespace.tables.get(t), sfNamespace)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Stage Files as Auto Loader Stream
# MAGIC 
# MAGIC 1. `read_append_only_stream(adls_location, table_name)`: provides an append only readstream which contains all cdc changes. This can be used to implement customer logic when going from raw to bronze. 

# COMMAND ----------

snowflakeStreamer = DatabricksSnowflakeReader()

# COMMAND ----------

user_name = spark.sql("SELECT current_user()").collect()[0][0]
checkpoint_path = f"/Users/{user_name}/snowflakecheckpoints/raw"
schema_path = f"/Users/{user_name}/snowflakeschemas/raw"
delta_table_name = "test_stream_table"

# COMMAND ----------

dbutils.fs.rm(checkpoint_path, True)
dbutils.fs.rm(schema_path, True)
spark.sql(f"DROP TABLE IF EXISTS {delta_table_name}")

# COMMAND ----------

table_data = f"{adls_location}/{stage_path}/{sfNamespace.snowflake_database}/{sfNamespace.snowflake_schema}/test_stream_table"
print(table_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Append Only CDC Tables 

# COMMAND ----------

raw_df = snowflakeStreamer.read_append_only_stream(spark, table_data, schema_path)
display(raw_df)

# COMMAND ----------

snowflakeStreamer.write_append_only_stream(raw_df, 'test_stream_table', checkpoint_path)

# COMMAND ----------

display(spark.sql(f"select * from {delta_table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge CDC Tables - replicates tables in Snowflake

# COMMAND ----------

user_name = spark.sql("SELECT current_user()").collect()[0][0]
checkpoint_path = f"/Users/{user_name}/snowflakecheckpoints/raw_merge"
schema_path = f"/Users/{user_name}/snowflakeschemas/raw_merge"
delta_table_name = "test_stream_table_merge"

# COMMAND ----------

raw_df = snowflakeStreamer.read_append_only_stream(spark, table_data, schema_path)
display(raw_df)

# COMMAND ----------

spark.sql(f"select * from test_stream_table").select("input_file_name").first().input_file_name.split("/")[-2]

# COMMAND ----------

from delta.tables import *
d = DeltaTable.forName(spark, 'asdfsafsd')

# COMMAND ----------

table_list = [t.tableName for t in spark.sql("show tables").select('tableName').collect()]

# COMMAND ----------

DeltaTable.isDeltaTable(spark, identifier='test_stream_table')

# COMMAND ----------

#
# NOT COMPLETED! OR TESTED!
#


def merge_snowflake_cdc_data(microBatchDF, batchId):
  microBatchDF = microBatchDF.withColumn("batchId", lit(batchId))
  table_name = microBatchDF.select("input_file_name").first().input_file_name.split("/")[-2]
  table_list = [t.tableName for t in spark.sql("show tables").select('tableName').collect()]
  
  # FIGURE THIS OUT
  match_keys = ["id"] # source.id = target.id  -- THIS NEEDS TO BE DYNAMIC SO WE DON'T HAVE THE METADATA ROWS
  
  if table_name not in table_list:
    (microBatchDF.write
     .format('delta')
     .saveAsTable(table_name)
    )
  else :
    microBatchDF.createOrReplaceTempView('source_vw')
    spark.sql("""
      MERGE INTO {} AS target 
      USING (SELECT * FROM source_vw) as source 
      ON {}
      -- updates - we will ignore when action = delete and update = true since they are the same row 
      WHEN MATCHED and source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = TRUE THEN 
          UPDATE SET target.string_value = source.string_value, target.modified_datetime = current_timestamp() -- THIS NEEDS TO BE DYNAMIC SO WE DON'T HAVE THE METADATA ROWS
      -- deletes 
      WHEN MATCHED and source.METADATA$ACTION = 'DELETE' and source.METADATA$ISUPDATE = FALSE THEN DELETE 

      -- inserts 
      WHEN NOT MATCHED and source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = FALSE THEN INSERT (id, string_value, modified_datetime) values (source.id, source.string_value, current_timestamp()) -- THIS NEEDS TO BE DYNAMIC SO WE DON'T HAVE THE METADATA ROWS
    """.format(table_name))
    
  

# COMMAND ----------

# MAGIC %md
# MAGIC Snowflake Merge statement
# MAGIC ```
# MAGIC -- complete the merge to the sink 
# MAGIC MERGE INTO test_cdc_sink_table AS target 
# MAGIC USING (SELECT * FROM rac_test_stream) as source 
# MAGIC ON source.id = target.id 
# MAGIC -- updates - we will ignore when action = delete and update = true since they are the same row 
# MAGIC WHEN MATCHED and source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = TRUE THEN 
# MAGIC     UPDATE SET target.string_value = source.string_value, target.modified_datetime = current_timestamp()
# MAGIC 
# MAGIC -- deletes 
# MAGIC WHEN MATCHED and source.METADATA$ACTION = 'DELETE' and source.METADATA$ISUPDATE = FALSE THEN DELETE 
# MAGIC 
# MAGIC -- inserts 
# MAGIC WHEN NOT MATCHED and source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = FALSE THEN INSERT (id, string_value, modified_datetime) values (source.id, source.string_value, current_timestamp()) ;
# MAGIC 
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC drop the metadata columns when merging... but how? probably some sort of dynamic string formatting 
# MAGIC 
# MAGIC 
# MAGIC All that is really left is to create the merge functions for structured streaming and potentially an example using DLT as well. 
# MAGIC 
# MAGIC 
# MAGIC All users to submit a configuration file. This should be a separate class to help with automation but is not a requirement. Users should be able to leverage the code as in this library. 

# COMMAND ----------


