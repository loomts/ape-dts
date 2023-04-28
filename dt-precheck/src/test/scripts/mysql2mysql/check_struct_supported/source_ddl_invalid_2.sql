create database precheck_it;
CREATE TABLE precheck_it.table_test_1(id integer, text varchar(10),primary key (id)); 
CREATE TABLE precheck_it.table_test_3(id integer, text varchar(10), f_id integer ,primary key (id), CONSTRAINT fk_test_3_1 FOREIGN KEY(f_id) REFERENCES table_test_1(id)); 
