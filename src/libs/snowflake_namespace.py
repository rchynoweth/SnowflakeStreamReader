class SnowflakeNamespace():

#   def __init__(self, file_format_name, sas_token, stage_name, storage_account_name, container_name, snowflake_database, snowflake_schema, additional_path = None):
  def __init__(self, config=None, config_file=None):
    # unable to load constructors in python
    # this is a workaround to either have config dict or config file 
    assert config is not None or config_file is not None
    if config_file:
      with open(config_file) as f:
        config = json.load(f)
    
    self.file_format_type = 'JSON' # only support JSON at this time 
    self.file_format_name=config.get('file_format_name')
    self.sas_token=config.get('sas_token') 
    self.stage_name=config.get('stage_name')
    self.storage_account_name=config.get('storage_account_name')
    self.container_name=config.get('container_name')
    self.additional_path=config.get('additional_path') 
    # these values for snowflake database and schema are for the stage and file format creation not for tables
    self.snowflake_database=config.get('snowflake_database')
    self.snowflake_schema=config.get('snowflake_schema')
    self.set_tables(config.get('tables')) 
    
  def set_tables(self, table_list):
    self.tables = {}
    if table_list is not None:
      for t in table_list:
        key = f"{t.database_name}__{t.schema_name}__{t.table_name}"
        self.tables[key] = t
        
  def add_table(self, snowflake_table):
    key = f"{snowflake_table.database_name}__{snowflake_table.schema_name}__{snowflake_table.table_name}"
    self.tables[key] = snowflake_table
    
  def get_table(self, database_name, schema_name, table_name):
    key = f"{database_name}__{schema_name}__{table_name}"
    self.tables.get(table_name)
    
  def delete_table(self, database_name, schema_name, table_name):
    key = f"{database_name}__{schema_name}__{table_name}"
    self.tables.pop(key, None)

  def update_table(self, snowflake_table):
    key = f"{snowflake_table.database_name}__{snowflake_table.schema_name}__{snowflake_table.table_name}"
    self.tables[key] = snowflake_table
    
#   def update_table_item(self, table_name, key, value):
#     self.tables[table_name][key] = value