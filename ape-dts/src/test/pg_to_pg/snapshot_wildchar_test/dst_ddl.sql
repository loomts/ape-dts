DROP SCHEMA IF EXISTS test_db_1 CASCADE;
DROP SCHEMA IF EXISTS test_db_2 CASCADE;
DROP SCHEMA IF EXISTS test_db_3 CASCADE;
DROP SCHEMA IF EXISTS test_db_4 CASCADE;
DROP SCHEMA IF EXISTS test_db_5 CASCADE;
DROP SCHEMA IF EXISTS other_test_db_1 CASCADE;

CREATE SCHEMA test_db_1;
CREATE SCHEMA test_db_2;
CREATE SCHEMA test_db_3;
CREATE SCHEMA test_db_4;
CREATE SCHEMA test_db_5;
CREATE SCHEMA other_test_db_1;

CREATE TABLE test_db_1.one_pk_no_uk_1 (pk serial, val numeric(20,8)); 
CREATE TABLE test_db_1.one_pk_no_uk_2 (pk serial, val numeric(20,8)); 

CREATE TABLE test_db_2.one_pk_no_uk_1 (pk serial, val numeric(20,8)); 
CREATE TABLE test_db_2.one_pk_no_uk_2 (pk serial, val numeric(20,8)); 

CREATE TABLE test_db_3.one_pk_no_uk_1 (pk serial, val numeric(20,8)); 
CREATE TABLE test_db_3.one_pk_no_uk_2 (pk serial, val numeric(20,8)); 

CREATE TABLE test_db_4.one_pk_no_uk_1 (pk serial, val numeric(20,8)); 
CREATE TABLE test_db_4.one_pk_no_uk_2 (pk serial, val numeric(20,8)); 

CREATE TABLE test_db_5.one_pk_no_uk_1 (pk serial, val numeric(20,8)); 
CREATE TABLE test_db_5.one_pk_no_uk_2 (pk serial, val numeric(20,8)); 

CREATE TABLE other_test_db_1.one_pk_no_uk_1 (pk serial, val numeric(20,8)); 
CREATE TABLE other_test_db_1.one_pk_no_uk_2 (pk serial, val numeric(20,8)); 
