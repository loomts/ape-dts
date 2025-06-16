DROP USER IF EXISTS 'test_create'@'%';
DROP USER IF EXISTS 'test_drop'@'%';
DROP USER IF EXISTS 'test_grant'@'%';
DROP USER IF EXISTS 'test_revoke'@'%';
DROP USER IF EXISTS 'test_role1'@'%';
DROP USER IF EXISTS 'test_role2'@'%';
DROP ROLE IF EXISTS 'role1';
DROP ROLE IF EXISTS 'role2';
DROP ROLE IF EXISTS 'role3';

DROP DATABASE IF EXISTS dcl_test_1;
CREATE DATABASE dcl_test_1;

CREATE TABLE dcl_test_1.tb1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ); 
CREATE TABLE dcl_test_1.tb2 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ); 

