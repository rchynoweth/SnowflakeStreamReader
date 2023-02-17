class SnowflakeTable():
  
  def __init__(self, database_name, schema_name, table_name, merge_keys=['METADATA$ROW_ID'], task_schedule='1 MINUTE', task_warehouse_size='XSMALL', enabled=True):
    self.database_name = database_name
    self.schema_name = schema_name
    self.table_name = table_name 
    self.merge_keys = merge_keys 
    self.task_schedule = task_schedule
    self.task_warehouse_size = task_warehouse_size
    self.enabled = enabled
  
  
  def get_merge_keys_as_string(self):
    return ','.join(self.merge_keys)