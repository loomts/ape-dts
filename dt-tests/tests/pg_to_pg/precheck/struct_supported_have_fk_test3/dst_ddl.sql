DROP SCHEMA IF EXISTS precheck_it_pg2pg_5_3 CASCADE;
DROP SCHEMA IF EXISTS precheck_it_pg2pg_5_3_2 CASCADE;

CREATE SCHEMA precheck_it_pg2pg_5_3;
CREATE SCHEMA precheck_it_pg2pg_5_3_2;

CREATE TABLE precheck_it_pg2pg_5_3.table_test_3(id integer, text varchar(10), f_id integer);