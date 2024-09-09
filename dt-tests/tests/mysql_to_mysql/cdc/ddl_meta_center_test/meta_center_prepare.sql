DROP DATABASE IF EXISTS test_db_1;
DROP DATABASE IF EXISTS test_db_2;
DROP DATABASE IF EXISTS test_db_3;
DROP DATABASE IF EXISTS test_db_4;
DROP DATABASE IF EXISTS `中文database!@#$%^&*()_+`;
CREATE DATABASE test_db_1;
CREATE DATABASE test_db_2;
CREATE DATABASE test_db_3;

CREATE TABLE test_db_1.tb_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ); 

CREATE TABLE test_db_1.rename_tb_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) );
CREATE TABLE test_db_1.rename_tb_2 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) );
CREATE TABLE test_db_1.rename_tb_3 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) );

CREATE TABLE test_db_1.drop_tb_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ); 
CREATE TABLE test_db_1.drop_tb_2 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ); 

CREATE TABLE test_db_1.truncate_tb_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ); 
CREATE TABLE test_db_1.truncate_tb_2 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ); 

CREATE TABLE test_db_2.truncate_tb_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ); 

CREATE TABLE test_db_3.tb_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ); 