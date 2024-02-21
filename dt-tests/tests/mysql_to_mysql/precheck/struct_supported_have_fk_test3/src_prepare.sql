DROP DATABASE IF EXISTS precheck_it_mysql2mysql_5_3;
DROP DATABASE IF EXISTS precheck_it_mysql2mysql_5_3_2;

CREATE DATABASE precheck_it_mysql2mysql_5_3;
CREATE DATABASE precheck_it_mysql2mysql_5_3_2;

CREATE TABLE precheck_it_mysql2mysql_5_3_2.table_test_1(id integer, text varchar(10),primary key (id)); 
CREATE TABLE precheck_it_mysql2mysql_5_3.table_test_3(id integer, text varchar(10), f_id integer ,primary key (id), CONSTRAINT fk_test_3_1 FOREIGN KEY(f_id) REFERENCES precheck_it_mysql2mysql_5_3_2.table_test_1(id)); 
