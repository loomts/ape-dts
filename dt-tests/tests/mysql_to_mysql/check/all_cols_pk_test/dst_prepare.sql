DROP DATABASE IF EXISTS test_db_1;

CREATE DATABASE test_db_1;

CREATE TABLE test_db_1.all_cols_pk (f_0 int, f_1 int,f_2 int, PRIMARY KEY (f_0, f_1, f_2));