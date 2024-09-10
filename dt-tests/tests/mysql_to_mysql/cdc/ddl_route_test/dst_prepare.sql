DROP DATABASE IF EXISTS dst_test_db_1;
DROP DATABASE IF EXISTS dst_test_db_2;
DROP DATABASE IF EXISTS dst_test_db_3;
DROP DATABASE IF EXISTS dst_test_db_4;
DROP DATABASE IF EXISTS `dst_中文database!@$%^&*()_+`;
CREATE DATABASE dst_test_db_1;
CREATE DATABASE dst_test_db_2;
CREATE DATABASE dst_test_db_3;

CREATE TABLE dst_test_db_1.tb_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ); 

CREATE TABLE dst_test_db_1.rename_tb_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) );
CREATE TABLE dst_test_db_1.rename_tb_2 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) );
CREATE TABLE dst_test_db_1.rename_tb_3 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) );

CREATE TABLE dst_test_db_1.drop_tb_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ); 
CREATE TABLE dst_test_db_1.drop_tb_2 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ); 

CREATE TABLE dst_test_db_1.truncate_tb_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ); 
INSERT INTO dst_test_db_1.truncate_tb_1 VALUES (1, 1);

CREATE TABLE dst_test_db_1.truncate_tb_2 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ); 
INSERT INTO dst_test_db_1.truncate_tb_2 VALUES (1, 1);

CREATE TABLE dst_test_db_2.truncate_tb_1 ( f_0 tinyint, f_1 smallint DEFAULT NULL, PRIMARY KEY (f_0) ); 
INSERT INTO dst_test_db_2.truncate_tb_1 VALUES (1, 1);

