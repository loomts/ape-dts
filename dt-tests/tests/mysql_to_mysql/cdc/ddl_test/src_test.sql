
INSERT INTO test_db_1.tb_1 VALUES (1,1);

-- add column
ALTER TABLE test_db_1.tb_1 ADD COLUMN f_2 smallint DEFAULT NULL;
ALTER TABLE test_db_1.tb_1 ADD COLUMN f_3 smallint DEFAULT NULL;

INSERT INTO test_db_1.tb_1 VALUES (2,2,2,2);

-- drop column
ALTER TABLE test_db_1.tb_1 DROP COLUMN f_2;

INSERT INTO test_db_1.tb_1 VALUES (3,3,3);

-- truncate table
TRUNCATE test_db_1.truncate_tb_1;
TRUNCATE TABLE test_db_1.truncate_tb_2;

-- rename table
-- ALTER TABLE test_db_1.tb_1 RENAME test_db_1.tb_2;
-- RENAME TABLE test_db_1.tb_2 TO test_db_1.tb_3;

-- drop table
DROP TABLE test_db_1.drop_tb_1;

-- drop database 
DROP DATABASE test_db_3;

-- create database
CREATE DATABASE test_db_4;

-- create table
CREATE TABLE test_db_2.tb_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, f_2 smallint DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

INSERT INTO test_db_2.tb_1 VALUES (1,1,1);

-- add index
ALTER TABLE test_db_2.tb_1 ADD INDEX idx_f_1 (f_1);

-- NOT supported ddl
CREATE INDEX idx_f_2 ON test_db_2.tb_1 (f_2);

-- RENAME TABLE products TO products_old, products_new TO products;

-- create database with special character
CREATE DATABASE `中文database!@#$%^&*()_+`;

-- create table with chinese character
CREATE TABLE `中文database!@#$%^&*()_+`.`中文` ( f_0 tinyint, f_1 smallint DEFAULT NULL, f_2 smallint DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

INSERT INTO `中文database!@#$%^&*()_+`.`中文` VALUES(1, 1, 1);
