-------------------- 
-- This notebook should be used as an example to manually set up the required objects in Snowflake. 
-- 1. Create database and schema 
-- 2. Create file format (json)
-- 3. Create external Snowflake stage 
  -- we use Azure and a SAS token but you can also create a Snowflake Storage Integration instead of using a SAS token. 
  -- https://docs.snowflake.com/en/sql-reference/sql/create-stage.html
-- 4. Create a table from one of the Snowflake sample datasets 
-- 5. Create Stream on table 
-- 6. Create Snowflake Task to copy data into the external stage  



-- Create Database and Schema 
CREATE DATABASE IF NOT EXISTS demo;
USE DATABASE demo;

CREATE SCHEMA IF NOT EXISTS rac_schema;
USE SCHEMA rac_schema;


-- Create a parquet file format to unload data 
CREATE OR REPLACE FILE FORMAT rac_json 
TYPE = JSON;

-- Create an external stage 
-- this is where I will unload data 
CREATE OR REPLACE STAGE rac_ext_stage_demo_json
URL = 'azure://<account>.blob.core.windows.net/<container>'
CREDENTIALS = (AZURE_SAS_TOKEN = '<SAS TOKEN>')
FILE_FORMAT = rac_json
;


-- create table from tpch data
create or replace table test_customer 
as select * from snowflake_sample_data.tpch_sf1.customer; 

-- create Snowflake Stream on the table 
CREATE OR REPLACE STREAM test_customer_stream
ON TABLE test_customer 
APPEND_ONLY = FALSE -- gives updates and deletes
SHOW_INITIAL_ROWS = TRUE ; -- for the initial rows for the first pull then only new/updated rows 


-- Create Snowflake Task 
CREATE OR REPLACE TASK rac_test_data_cdc_unload 
SCHEDULE = '1 MINUTE'
ALLOW_OVERLAPPING_EXECUTION = FALSE 
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AS 
-- copy into @rac_ext_stage_demo_json/<path>/<database>/<schema>/<table_name>
copy into @rac_ext_stage_demo_json/cdc_unload/demo/rac_schema/rac_test
FROM (
  SELECT OBJECT_CONSTRUCT(*) as row_value FROM (SELECT *, current_timestamp() as load_datetime FROM rac_test_stream )
  )
PARTITION BY ('year=' || to_varchar(current_date(), 'YYYY') || '/month=' || to_varchar(current_date(), 'MM') || '/day=' || to_varchar(current_date(), 'DD')) -- set it up in the /yyyy/mm/dd format for autoloader
include_query_id=true ; -- ensures unique file names   


-- start task
alter task if exists rac_test_data_cdc_unload resume;

-- halt task
alter task if exists rac_test_data_cdc_unload suspend;
