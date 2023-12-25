DROP SCHEMA IF EXISTS precheck_it_pg2pg_5_2;

CREATE SCHEMA precheck_it_pg2pg_5_2;

CREATE TABLE precheck_it_pg2pg_5_2.table_test_1(id integer, text varchar(10),primary key (id)); 
CREATE TABLE precheck_it_pg2pg_5_2.table_test_2(id integer, text varchar(10),primary key (id)); 
CREATE TABLE precheck_it_pg2pg_5_2.table_test_3(id integer, text varchar(10), f_id integer, f_id2 integer ,primary key (id), CONSTRAINT fk_test_3_1 FOREIGN KEY(f_id) REFERENCES precheck_it_pg2pg_5_2.table_test_1(id), CONSTRAINT fk_test_3_2 FOREIGN KEY(f_id2) REFERENCES precheck_it_pg2pg_5_2.table_test_2(id)); 
