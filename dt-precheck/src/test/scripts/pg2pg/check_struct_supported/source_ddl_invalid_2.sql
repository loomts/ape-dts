create schema precheck_it;
CREATE TABLE precheck_it.table_test_1(id integer, text varchar(10),primary key (id)); 
CREATE TABLE precheck_it.table_test_3(id integer, text varchar(10), f_id integer ,primary key (id), constraint table_test_uk_1 unique(f_id), FOREIGN KEY (f_id) REFERENCES precheck_it.table_test_1 (id)); 