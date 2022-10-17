# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables - Pipeline Example 
# MAGIC 
# MAGIC We will cover how to build a DLT pipeline that can consume and format your Snowflake data. 
# MAGIC 
# MAGIC This example simply scans a directory location and loads the tables. However, the columns which we need to merge on need to be provided. Right now this simply hard codes the key to `id` for all tables. One could create a `metadata_table` and use that to dynamically look of merge keys for each table. 
# MAGIC 
# MAGIC There is another sample DLT pipeline that uses the class objects used for the structured streaming example. These would be used with configuration files or defined in the code itself. 
# MAGIC 
# MAGIC Resources:  
# MAGIC - [`apply_changes_into`](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-cdc.html)

# COMMAND ----------

import os
import dlt
from pyspark.sql.functions import *
# from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Set data location for tables
data_path = "abfss://snowflakedemo@racadlsgen2.dfs.core.windows.net/cdc_demo"

# COMMAND ----------

# DBTITLE 1,List databases, schemas, and tables
databases = [d.name[0:-1] for d in dbutils.fs.ls(data_path) if d.name[0] != '_']
schemas = []
tables = [] 

for d in databases:
  schemas += [(d, s.name[0:-1]) for s in dbutils.fs.ls(f"{data_path}/{d}") if s.name[0] != '_']
  
for s in schemas:
  tables += [(s[0], s[1], t.name[0:-1]) for t in dbutils.fs.ls(f"{data_path}/{s[0]}/{s[1]}") if t.name[0] != '_']
  
tables

# COMMAND ----------

# DBTITLE 1,Function to create append only tables
### 
# This creates append only tables for our bronze sources
# we can do further modeling in silver/gold layers 
###
def generate_tables(table_tuple):
  @dlt.table(
    name=f"{table_tuple[2]}_stream",
    comment="BRONZE: {}".format(table_tuple[2])
  )
  def create_table():
    return (
      spark.readStream.format('cloudfiles')
        .option('cloudFiles.format', 'json')
        .load(f"{data_path}/{table_tuple[0]}/{table_tuple[1]}/{table_tuple[2]}/*/*/*/*.json.gz")
        .withColumn('input_file', input_file_name())
        .withColumn('system_ts', to_timestamp(col("load_datetime")))
        .withColumnRenamed('METADATA$ISUPDATE', 'METADATA_ISUPDATE') # remove dollar signs from the column names 
        .withColumnRenamed('METADATA$ACTION', 'METADATA_ACTION')
        .withColumnRenamed('METADATA$ROW_ID', 'METADATA_ROW_ID')
        .filter("(METADATA_ACTION != 'DELETE' or METADATA_ISUPDATE != True)") # filters out cdc rows that we do not want or need 
    )

# COMMAND ----------

# DBTITLE 1,Pass the list of table tuples to the function 
for t in tables:
  generate_tables(t)

# COMMAND ----------

def generate_merge_tables(table_tuple):
  dlt.create_streaming_live_table(table_tuple[2])
  
  dlt.apply_changes(
    target = table_tuple[2],
    source = f"{table_tuple[2]}_stream",
    keys = ["id"],
    sequence_by = "system_ts",
    apply_as_deletes = "(METADATA_ACTION != 'DELETE' or METADATA_ISUPDATE != False)",
    except_column_list = ["METADATA_ACTION", "METADATA_ISUPDATE", "METADATA_ROW_ID"]
  )

# COMMAND ----------

for t in tables:
  generate_merge_tables(t)

# COMMAND ----------


