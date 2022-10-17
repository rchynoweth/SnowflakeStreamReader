# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables - Pipeline Example 
# MAGIC 
# MAGIC We will cover how to build a DLT pipeline that can consume and format your Snowflake data. 
# MAGIC 
# MAGIC Resources:  
# MAGIC - [`apply_changes_into`](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-cdc.html)

# COMMAND ----------

import os
import dlt
from pyspark.sql.functions import *
# from libs.snowflake_table import SnowflakeTable # python imports not supported at this time. This would need to be a pip install as of 10/15/22. 

# COMMAND ----------

# DBTITLE 1,Set data location for tables
data_path = "abfss://snowflakedemo@racadlsgen2.dfs.core.windows.net/cdc_demo"

# COMMAND ----------

# DBTITLE 1,List databases, schemas, and tables
# t1 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_stream_table1", merge_keys=['id'])
# t2 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_stream_table2", merge_keys=['id', 'id2'])
# t3 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_stream_table3", merge_keys=['id'])
# table_list = [t1,t2,t3]

# temporary until python inports work for dlt
class SnowflakeTable():
  def __init__(self, database_name, schema_name, table_name, merge_keys):
    self.database_name = database_name
    self.schema_name = schema_name
    self.table_name = table_name
    self.merge_keys = merge_keys


t1 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_customer", merge_keys=['c_custkey'])
t2 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_lineitem", merge_keys=['l_orderkey', 'l_linenumber'])
t3 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_nation", merge_keys=['n_nationkey'])
t4 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_orders", merge_keys=['o_orderkey'])
t5 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_part", merge_keys=['p_partkey'])
t6 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_partsupp", merge_keys=['ps_partkey', 'ps_suppkey'])
t7 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_region", merge_keys=['r_regionkey'])
t8 = SnowflakeTable(database_name="demo", schema_name="rac_schema",table_name="test_supplier", merge_keys=['s_suppkey'])
tables = [t1,t2,t3,t4,t5,t6,t7,t8]

# COMMAND ----------

### In the Spark Structured Streaming example, we needed to use a window function 
##### to find the latest row by key(s) if we were loading multiple files 
##### with the built-in support for scd type 1 tables which are ordered by
##### a datetime column makes this process simple.  


# merge_keys = microBatchDF.select("merge_keys").first().merge_keys.split(",")  
# w = Window.partitionBy(merge_keys).orderBy(desc('system_ts'))
# microBatchDF = (microBatchDF.withColumn("batchId", lit(batchId))
#                 .withColumnRenamed('METADATA$ISUPDATE', 'METADATA_ISUPDATE')
#                 .withColumnRenamed('METADATA$ACTION', 'METADATA_ACTION')
#                 .withColumnRenamed('METADATA$ROW_ID', 'METADATA_ROW_ID')
#                 .withColumn('group_rank', dense_rank().over(w))
#                 .filter(col("group_rank")==1)
#                 .drop("group_rank")
#                 .drop("merge_keys")
#                )

# COMMAND ----------

# DBTITLE 1,Function to create append only tables
### 
# This creates append only tables for our bronze sources
# we can do further modeling in silver/gold layers 
###
def generate_tables(tbl):
  @dlt.table(
    name=f"{tbl.table_name}_stream",
    comment="BRONZE: {}".format(tbl.table_name)
  )
  def create_table():
    return (
      spark.readStream.format('cloudfiles')
        .option('cloudFiles.format', 'json')
        .load(f"{data_path}/{tbl.database_name}/{tbl.schema_name}/{tbl.table_name}/*/*/*/*.json.gz")
        .withColumn('input_file_name', input_file_name())
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

def generate_merge_tables(tbl):
  dlt.create_streaming_live_table(tbl.table_name)

  dlt.apply_changes(
    target = tbl.table_name,
    source = f"{tbl.table_name}_stream",
    keys = tbl.merge_keys,
    sequence_by = col("system_ts"),
    apply_as_deletes = expr("METADATA_ACTION ='DELETE' "),
    except_column_list = ["METADATA_ACTION", "METADATA_ISUPDATE", "METADATA_ROW_ID", "year", "month", "day", "_rescued_data"],
    stored_as_scd_type=1
  )

# COMMAND ----------

for t in tables:
  generate_merge_tables(t)
