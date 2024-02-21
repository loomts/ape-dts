drop schema if exists precheck_it_pg2pg_6 cascade;
create schema precheck_it_pg2pg_6;

CREATE TABLE precheck_it_pg2pg_6.table_with_unique_constraint (id INTEGER, name VARCHAR(50), CONSTRAINT uk_name UNIQUE (name));
