from pyspark.sql.functions import *
from delta.tables import *
from pyspark.sql.window import Window


class DatabricksSnowflakeReader():
  
  def __init__(self, spark):
    self.spark = spark
  
  def get_table_schema_location(self, snowflake_table, snowflake_namespace):
    schema_location = f"abfss://{snowflake_namespace.container_name}@{snowflake_namespace.storage_account_name}.dfs.core.windows.net/" + \
            f"{snowflake_namespace.additional_path}/_schemas/{snowflake_table.database_name}/{snowflake_table.schema_name}/{snowflake_table.table_name}"
    return schema_location
    
  def get_table_checkpoint_location(self, snowflake_table, snowflake_namespace):
    checkpoint_location = f"abfss://{snowflake_namespace.container_name}@{snowflake_namespace.storage_account_name}.dfs.core.windows.net/" + \
            f"{snowflake_namespace.additional_path}/_checkpoints/{snowflake_table.database_name}/{snowflake_table.schema_name}/{snowflake_table.table_name}"
    return checkpoint_location
  
  def get_data_path(self, snowflake_table, snowflake_namespace):
    data_path = f"abfss://{snowflake_namespace.container_name}@{snowflake_namespace.storage_account_name}.dfs.core.windows.net/" + \
            f"{snowflake_namespace.additional_path}/{snowflake_table.database_name}/{snowflake_table.schema_name}/{snowflake_table.table_name}/****/**/**/*.json.gz"
    return data_path
    
  
  def read_append_only_stream(self, dir_location, schema_path):
    return (self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", schema_path)
            .load(dir_location)
            .withColumn('system_ts', to_timestamp(col("load_datetime")))
            .withColumn('input_file_name', input_file_name())
           )


  def write_append_only_stream(self, input_df, table_name, checkpoint_path):
    (input_df.writeStream
     .format('delta')
     .option("checkpointLocation", checkpoint_path)
     .toTable(table_name)
    )
  
  def read_merge_stream(self, dir_location, schema_path, merge_keys=""):
    """
    Reads a directory of json files and returns a streaming dataframe 
    """
    return (self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", schema_path)
            .load(dir_location)
            .withColumn('system_ts', to_timestamp(col("load_datetime")))
            .withColumn('input_file_name', input_file_name())
            .withColumn('merge_keys', lit(merge_keys))
           )
    

  def write_merge_stream(self, microBatchDF, batchId):
    """
    A function to be used in a foreachBatch writeStream where the input dataframe is a 
    micro-batch with the output schema of the DF returned in the read_merge_stream function 
    """
    # in the case of loading multiple files we apply a window function based on the merge keys 
    merge_keys = microBatchDF.select("merge_keys").first().merge_keys.split(",")  
    w = Window.partitionBy(merge_keys).orderBy(desc('system_ts'))
    microBatchDF = (microBatchDF.withColumn("batchId", lit(batchId))
                    .withColumnRenamed('METADATA$ISUPDATE', 'METADATA_ISUPDATE')
                    .withColumnRenamed('METADATA$ACTION', 'METADATA_ACTION')
                    .withColumnRenamed('METADATA$ROW_ID', 'METADATA_ROW_ID')
                    .withColumn('group_rank', dense_rank().over(w))
                    .filter(col("group_rank")==1)
                    .drop("group_rank")
                    .drop("merge_keys")
                   )

    # get the table name and list of catalog tables 
    table_name = microBatchDF.select("input_file_name").first().input_file_name.split("/")[-5]
    table_list = [t.tableName for t in self.spark.sql("show tables").select('tableName').collect()]
    # snowflake metadata columns that we won't need 
    drop_cols = ['METADATA_ISUPDATE', 'METADATA_ACTION', 'METADATA_ROW_ID']

    ### variables used in the merge statement 
    # join string
    key_string = ''.join([f"source.{k} = target.{k} and " for k in merge_keys])[0:-5]
    # matched update string 
    update_string = ''.join([f"target.{c} = source.{c}," for c in microBatchDF.columns if c not in drop_cols] )[0:-1]
    # columns we will insert
    insert_cols = ''.join([f"{c}," for c in microBatchDF.columns if c not in drop_cols] )[0:-1]
    # column values we will insert
    insert_string = ''.join([f"source.{c}," for c in microBatchDF.columns if c not in drop_cols] )[0:-1]

    # filter out rows we don't need
    # Snowflake Streams provides two rows for each update while inserts/deletes are a single row 
    microBatchDF = microBatchDF.filter("(METADATA_ACTION != 'DELETE' or METADATA_ISUPDATE != True)")

    # if table does not exist then create it
    if table_name not in table_list:
      (microBatchDF.drop(drop_cols[0], drop_cols[1], drop_cols[2])
       .write
       .format('delta')
       .saveAsTable(table_name)
      )
    else :
      # make DF referenceable in SQL 
      microBatchDF.createOrReplaceTempView(f"{table_name}_{batchId}_vw")
      # Complete the merge 
      # 'microBatchDF_jdf.sparkSession.sql' is used instead of spark SQL due to the temp view 
      microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO {} AS target 
       USING (SELECT * FROM {}_{}_vw) as source 
        ON {}  
        -- updates 
        WHEN MATCHED and source.METADATA_ACTION = 'INSERT' and source.METADATA_ISUPDATE = TRUE THEN 
            UPDATE SET {}
        -- deletes 
        WHEN MATCHED and source.METADATA_ACTION = 'DELETE' and source.METADATA_ISUPDATE = FALSE THEN DELETE 

        -- inserts 
        WHEN NOT MATCHED and source.METADATA_ACTION = 'INSERT' and source.METADATA_ISUPDATE = FALSE THEN INSERT ( {} ) values ( {} ) 
      """.format(table_name, table_name, batchId, key_string, update_string, insert_cols, insert_string) )

      # Drop the temp view - likely unnecessary 
      self.spark.sql(f"DROP VIEW IF EXISTS {table_name}_{batchId}_vw")



