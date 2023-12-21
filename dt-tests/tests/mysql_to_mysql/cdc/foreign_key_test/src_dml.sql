INSERT INTO test_db_1.tb_1 VALUES (1, 1, 1, 1);
INSERT INTO test_db_1.tb_1 VALUES (2, 2, 2, 2);

INSERT INTO test_db_1.fk_tb_1 VALUES (1, 1, 1, 1);
INSERT INTO test_db_1.fk_tb_1 VALUES (2, 2, 2, 2);

INSERT INTO test_db_1.fk_tb_2 VALUES (1, 1, 1, 1);
INSERT INTO test_db_1.fk_tb_2 VALUES (2, 2, 2, 2);

INSERT INTO test_db_1.fk_tb_3 VALUES (1, 1, 1, 1);
INSERT INTO test_db_1.fk_tb_3 VALUES (2, 2, 2, 2);

UPDATE test_db_1.tb_1 SET f_3 = 5;
UPDATE test_db_1.fk_tb_1 SET f_3 = 5;
UPDATE test_db_1.fk_tb_2 SET f_3 = 5;
UPDATE test_db_1.fk_tb_3 SET f_3 = 5;

DELETE FROM test_db_1.tb_1;
DELETE FROM test_db_1.fk_tb_3;
DELETE FROM test_db_1.fk_tb_2;
DELETE FROM test_db_1.fk_tb_1;