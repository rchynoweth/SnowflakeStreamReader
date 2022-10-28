---- CREATE BASE OBJECTS
use demo;
use schema rac_schema; 

create or replace table test_customer 
as select * from snowflake_sample_data.tpch_sf1.customer; 

create or replace table test_lineitem
as select * from snowflake_sample_data.tpch_sf1.lineitem; 

create or replace table test_nation
as select * from snowflake_sample_data.tpch_sf1.nation; 

create or replace table test_orders
as select * from snowflake_sample_data.tpch_sf1.orders; 

create or replace table test_part
as select * from snowflake_sample_data.tpch_sf1.part; 

create or replace table test_partsupp 
as select * from snowflake_sample_data.tpch_sf1.partsupp; 

create or replace table test_region
as select * from snowflake_sample_data.tpch_sf1.region;

create or replace table test_supplier
as select * from snowflake_sample_data.tpch_sf1.supplier; 




select * from test_customer;

---- Insert, Updates, and Deletes 
insert into test_customer values 
(150001, 'TEST USER 1', 'TEST ADDRESS 1', '14', '12-123-123-1234', 826.65, 'HOUSEHOLD', 'This is a comment.'),
(150002, 'TEST USER 2', 'TEST ADDRESS 2', '9', '12-123-123-1234', 826.65, 'HOUSEHOLD', 'This is a comment.'),
(150003, 'TEST USER 3', 'TEST ADDRESS 3', '2', '12-123-123-1234', 826.65, 'HOUSEHOLD', 'This is a comment.'),
(150004, 'TEST USER 4', 'TEST ADDRESS 4', '22', '12-123-123-1234', 826.65, 'HOUSEHOLD', 'This is a comment.'),
(150005, 'TEST USER 5', 'TEST ADDRESS 5', '12', '12-123-123-1234', 826.65, 'AUTOMOBILE', 'This is a comment.'),
(150006, 'TEST USER 6', 'TEST ADDRESS 6', '10', '12-123-123-1234', 826.65, 'BUILDING', 'This is a comment.');

select * from test_customer_stream;

UPDATE TEST_CUSTOMER SET C_NAME = 'TEST NAME UPDATE' WHERE c_custkey in (150001, 150002); 

select * from test_customer_stream;

DELETE FROM TEST_CUSTOMER WHERE C_CUSTKEY > 150000;

select * from test_customer_stream;


---- Suspend all tasks that we created for the dmeo 
alter task test_customer_stream_task suspend;
alter task test_lineitem_stream_task suspend;
alter task test_nation_stream_task suspend;
alter task test_orders_stream_task suspend;
alter task test_part_stream_task suspend;
alter task test_partsupp_stream_task suspend;
alter task test_region_stream_task suspend;
alter task test_supplier_stream_task suspend;



----- CLEAN UP CODE 
drop file format if exists rac_json;
drop stage if exists RAC_EXT_STAGE_DEMO_JSON;

drop task if exists test_customer_stream_task ;
drop task if exists test_lineitem_stream_task;
drop task if exists test_nation_stream_task;
drop task if exists test_orders_stream_task;
drop task if exists test_part_stream_task;
drop task if exists test_partsupp_stream_task;
drop task if exists test_region_stream_task;
drop task if exists test_supplier_stream_task;
