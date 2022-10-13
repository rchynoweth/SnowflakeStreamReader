class SnowflakeTable():
  
  def __init__(self, database_name, schema_name, table_name, merge_keys=None, task_schedule='1 MINUTE', task_warehouse_size='XSMALL'):
    self.database_name = database_name
    self.schema_name = schema_name
    self.table_name = name 
    self.merge_keys = merge_keys 
    self.task_schedule = task_schedule
    self.task_warehouse_size = task_warehouse_size
  
  
