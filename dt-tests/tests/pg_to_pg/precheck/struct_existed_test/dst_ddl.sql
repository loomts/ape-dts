drop schema if exists precheck_it_pg2pg_1 cascade;
create schema precheck_it_pg2pg_1;

CREATE TABLE precheck_it_pg2pg_1.table_test(id integer, text varchar(10),primary key (id)); 