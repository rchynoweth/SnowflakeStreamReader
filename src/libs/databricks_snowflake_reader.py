from pyspark.sql.functions import to_timestamp, col, input_file_name
from delta.tables import *

class DatabricksSnowflakeReader():
  
  def read_append_only_stream(self, spark, dir_location, schema_path):
    return (spark.readStream
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