create schema precheck_it_pg2pg_2_1;
CREATE TABLE precheck_it_pg2pg_2_1.table_test_1(id integer, text varchar(10),primary key (id)); 
CREATE TABLE precheck_it_pg2pg_2_1.table_test_3(id integer, text varchar(10), f_id integer ,primary key (id), constraint table_test_uk_1 unique(f_id), FOREIGN KEY (f_id) REFERENCES precheck_it_pg2pg_2_1.table_test_1 (id)); 