drop database if exists test_db_1;

create database if not exists test_db_1;

CREATE TABLE test_db_1.tb_1(f_0 int, f_1 int, f_2 int, f_3 int, PRIMARY KEY(f_0));

-- foreign constraints
CREATE TABLE test_db_1.fk_tb_1 (f_0 int, f_1 int UNIQUE, f_2 int UNIQUE, f_3 int, PRIMARY KEY(f_0));

CREATE TABLE test_db_1.fk_tb_2 (f_0 int, f_1 int UNIQUE, f_2 int UNIQUE, f_3 int, PRIMARY KEY(f_0));

CREATE TABLE test_db_1.fk_tb_3 (f_0 int, f_1 int UNIQUE, f_2 int UNIQUE, f_3 int, PRIMARY KEY(f_0));

ALTER TABLE test_db_1.fk_tb_2 ADD CONSTRAINT fk_tb_2_1 FOREIGN KEY (f_1) REFERENCES test_db_1.fk_tb_1 (f_1);
ALTER TABLE test_db_1.fk_tb_2 ADD CONSTRAINT fk_tb_2_2 FOREIGN KEY (f_2) REFERENCES test_db_1.fk_tb_1 (f_2);

ALTER TABLE test_db_1.fk_tb_3 ADD CONSTRAINT fk_tb_3_1 FOREIGN KEY (f_1) REFERENCES test_db_1.fk_tb_2 (f_2);
