DROP DATABASE IF EXISTS precheck_it_mysql2mysql_6;

CREATE DATABASE precheck_it_mysql2mysql_6;

CREATE TABLE precheck_it_mysql2mysql_6.table_with_unique_constraint (id INT, name VARCHAR(50), name2 VARCHAR(50), UNIQUE KEY uk_name (name, name2));