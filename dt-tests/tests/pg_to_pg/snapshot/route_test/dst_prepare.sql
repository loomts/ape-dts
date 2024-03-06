DROP SCHEMA IF EXISTS dst_test_db_1 CASCADE;
DROP SCHEMA IF EXISTS test_db_2 CASCADE;
DROP SCHEMA IF EXISTS dst_test_db_2 CASCADE;
DROP SCHEMA IF EXISTS test_db_3 CASCADE;
DROP SCHEMA IF EXISTS dst_test_db_3 CASCADE;

CREATE SCHEMA dst_test_db_1;
CREATE SCHEMA test_db_2;
CREATE SCHEMA dst_test_db_2;
CREATE SCHEMA test_db_3;
CREATE SCHEMA dst_test_db_3;

-- db map
CREATE TABLE dst_test_db_1.one_pk_no_uk_1 ( f_0 serial, f_1 numeric(20,8), PRIMARY KEY (f_0) ); 
CREATE TABLE dst_test_db_1.one_pk_no_uk_2 ( f_0 serial, f_1 numeric(20,8), PRIMARY KEY (f_0) ); 

-- tb map
CREATE TABLE dst_test_db_2.dst_one_pk_no_uk_1 ( f_0 serial, f_1 numeric(20,8), PRIMARY KEY (f_0) ); 
-- no map
CREATE TABLE test_db_2.one_pk_no_uk_2 ( f_0 serial, f_1 numeric(20,8), PRIMARY KEY (f_0) ); 

-- col map
CREATE TABLE dst_test_db_3.dst_one_pk_no_uk_1 ( dst_f_0 serial, dst_f_1 numeric(20,8), PRIMARY KEY (dst_f_0) ); 
-- no map
CREATE TABLE test_db_3.one_pk_no_uk_2 ( f_0 serial, f_1 numeric(20,8), PRIMARY KEY (f_0) ); 
