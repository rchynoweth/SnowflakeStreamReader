-- Create Database and Schema 
CREATE DATABASE IF NOT EXISTS demo;
USE DATABASE demo;

CREATE SCHEMA IF NOT EXISTS rac_schema;
USE SCHEMA rac_schema;

----------------- CREATE TABLE AND INSERT VALUES 

---- one
CREATE OR REPLACE TABLE test_stream_table1 
(
    id int IDENTITY(1,1),
    string_value string, 
    modified_datetime timestamp default current_timestamp()
); 

INSERT INTO test_stream_table1 (string_value)
VALUES ('test_one'), ('test_two'), ('test_three'), ('test_four'),('test_five'),('test_six'),('test_seven'),('test_eight'),('test_nine'),('test_ten');


---- two
CREATE OR REPLACE TABLE test_stream_table2 
(
    id int IDENTITY(1,1),
    id2 int IDENTITY(1,1),
    string_value string, 
    modified_datetime timestamp default current_timestamp()
); 

INSERT INTO test_stream_table2 (string_value)
VALUES ('test_one'), ('test_two'), ('test_three'), ('test_four'),('test_five'),('test_six'),('test_seven'),('test_eight'),('test_nine'),('test_ten');


---- three  
CREATE OR REPLACE TABLE test_stream_table3
(
    id int IDENTITY(1,1),
    string_value string, 
    modified_datetime timestamp default current_timestamp()
); 

INSERT INTO test_stream_table3 (string_value)
VALUES ('test_one'), ('test_two'), ('test_three'), ('test_four'),('test_five'),('test_six'),('test_seven'),('test_eight'),('test_nine'),('test_ten');


















--------- stop tasks 

alter task test_stream_table1_stream_task suspend;
alter task test_stream_table2_stream_task suspend;
alter task test_stream_table3_stream_task suspend;






