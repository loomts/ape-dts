DROP TABLE IF EXISTS tb_1;
DROP TABLE IF EXISTS fk_tb_3;
DROP TABLE IF EXISTS fk_tb_2;
DROP TABLE IF EXISTS fk_tb_1;

CREATE TABLE tb_1(f_0 int, f_1 int, f_2 int, f_3 int, PRIMARY KEY(f_0));

-- foreign constraints
CREATE TABLE fk_tb_1 (f_0 int, f_1 int UNIQUE, f_2 int UNIQUE, f_3 int, PRIMARY KEY(f_0));

CREATE TABLE fk_tb_2 (f_0 int, f_1 int UNIQUE, f_2 int UNIQUE, f_3 int, PRIMARY KEY(f_0));

CREATE TABLE fk_tb_3 (f_0 int, f_1 int UNIQUE, f_2 int UNIQUE, f_3 int, PRIMARY KEY(f_0));

ALTER TABLE fk_tb_2 ADD CONSTRAINT fk_tb_2_1 FOREIGN KEY (f_1) REFERENCES fk_tb_1 (f_1);
ALTER TABLE fk_tb_2 ADD CONSTRAINT fk_tb_2_2 FOREIGN KEY (f_2) REFERENCES fk_tb_1 (f_2);

ALTER TABLE fk_tb_3 ADD CONSTRAINT fk_tb_3_1 FOREIGN KEY (f_1) REFERENCES fk_tb_2 (f_2);
