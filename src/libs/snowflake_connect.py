import snowflake.connector 

class SnowflakeConnect():
  
  def __init__(self, snowflake_creds):
    self.snowflake_creds = snowflake_creds
    self.con = snowflake.connector.connect(user=snowflake_creds.get('user'),
                                           password=snowflake_creds.get('password'),
                                           account=snowflake_creds.get('snowflake_account')
                                          )
    
  def run_query(self, text):
    return self.con.cursor().execute(text).sfqid
  
  
  def get_query_status(self, sfqid):
    return self.con.get_query_status(sfqid)
  
    
  def create_file_format(self, name, type='JSON'):
    return self.run_query(f"CREATE OR REPLACE FILE FORMAT {name} TYPE = {type};")
    
    
  def create_external_stage(self, snowflake_namespace):
    stage_name=snowflake_namespace.stage_name
    file_format_name=snowflake_namespace.file_format_name
    sas_token=snowflake_namespace.sas_token
    storage_account_name=snowflake_namespace.storage_account_name
    container_name=snowflake_namespace.container_name
    additional_path=snowflake_namespace.additional_path 
    additional_path = "" if additional_path is None else additional_path 
    
    return self.run_query(f"""
        CREATE OR REPLACE STAGE {stage_name}
        URL = 'azure://{storage_account_name}.blob.core.windows.net/{container_name}/{additional_path}'
        CREDENTIALS = (AZURE_SAS_TOKEN = '{sas_token}')
        FILE_FORMAT = {file_format_name}
    """)
    
    
  def create_snowflake_stream(self, snowflake_table):
    database_name = snowflake_table.database_name
    schema_name = snowflake_table.schema_name 
    table_name = snowflake_table.table_name 
    
    return self.run_query(f"""
        CREATE OR REPLACE STREAM {database_name}.{schema_name}.{table_name}_stream
        ON TABLE {database_name}.{schema_name}.{table_name} 
        APPEND_ONLY = FALSE -- gives updates and deletes
        SHOW_INITIAL_ROWS = TRUE ; -- for the initial rows for the first pull then only new/updated rows 
    """)
    
    
  def create_snowflake_task(self, snowflake_table, snowflake_namespace):
    database_name = snowflake_table.database_name
    schema_name = snowflake_table.schema_name 
    table_name = snowflake_table.table_name 
    stage_name = f"{snowflake_namespace.snowflake_database}.{snowflake_namespace.snowflake_schema}.{snowflake_namespace.stage_name}"
    
    
    return self.run_query(f"""
        CREATE OR REPLACE TASK {database_name}.{schema_name}.{table_name}_stream_task
        SCHEDULE = '{snowflake_table.task_schedule}' -- Change as needed 
        ALLOW_OVERLAPPING_EXECUTION = FALSE -- if they overlap then we may get duplicates from the stream if the previous DML is not complete 
        USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = '{snowflake_table.task_warehouse_size}' -- using Snowflake Serverless compute 
        AS 
          COPY INTO @{stage_name}/{database}/{schema}/{table_name}
          FROM (
          SELECT OBJECT_CONSTRUCT(*) as row_value FROM (SELECT *, current_timestamp() as load_datetime FROM {table_name}_stream )
              )
          partition by ('year=' || to_varchar(current_date(), 'YYYY') || '/month=' || to_varchar(current_date(), 'MM') || '/day=' || to_varchar(current_date(), 'DD')) -- set it up in the /yyyy/mm/dd format for autoloader
          include_query_id=true;  -- Ensures that each file we write has a unique name which is required for auto loader  
        
    """)

  def account_setup(self, snowflake_namespace):
    self.run_query(f"USE DATABASE {snowflake_namespace.snowflake_database}")
    self.run_query(f"USE SCHEMA {snowflake_namespace.snowflake_schema}")
    
    file_query_id = self.create_file_format(snowflake_namespace.file_format_name, snowflake_namespace.file_format_type)
    stage_query_id = self.create_external_stage(stage_name=snowflake_namespace.stage_name, 
                                                file_format_name=snowflake_namespace.file_format_name, 
                                                sas_token=snowflake_namespace.sas_token, 
                                                storage_account_name=snowflake_namespace.storage_account_name, 
                                                container_name=snowflake_namespace.container_name, 
                                                additional_path=snowflake_namespace.additional_path)
    return file_query_id, stage_query_id
  
  
  
  def set_task_status(self, snowflake_table, status='RESUME'):
    database_name = snowflake_table.database_name
    schema_name = snowflake_table.schema_name 
    table_name = snowflake_table.table_name 
    self.run_query(f"ALTER TASK IF EXISTS {database_name}.{schema_name}.{table_name}_stream_task {status}")
  
    
  def table_setup(self, snowflake_table, snowflake_namespace):
    stream_query_id = self.create_snowflake_stream(snowflake_table)
    task_query_id = self.create_snowflake_task(snowflake_table, snowflake_namespace)
    self.set_task_status(snowflake_table=snowflake_table, status='RESUME')
    return stream_query_id, task_query_id
  