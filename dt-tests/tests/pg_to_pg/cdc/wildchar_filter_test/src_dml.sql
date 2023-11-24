INSERT INTO test_db_1.one_pk_no_uk_1 VALUES (1,2);
INSERT INTO test_db_1.one_pk_no_uk_2 VALUES (2,20);

UPDATE test_db_1.one_pk_no_uk_1 SET val=30;
UPDATE test_db_1.one_pk_no_uk_2 SET val=30;

DELETE FROM test_db_1.one_pk_no_uk_1;
DELETE FROM test_db_1.one_pk_no_uk_2;

INSERT INTO test_db_2.one_pk_no_uk_1 VALUES (1,2);
INSERT INTO test_db_2.one_pk_no_uk_2 VALUES (2,20);

UPDATE test_db_2.one_pk_no_uk_1 SET val=30;
UPDATE test_db_2.one_pk_no_uk_2 SET val=30;

DELETE FROM test_db_2.one_pk_no_uk_1;
DELETE FROM test_db_2.one_pk_no_uk_2;

INSERT INTO test_db_3.one_pk_no_uk_1 VALUES (1,2);
INSERT INTO test_db_3.one_pk_no_uk_2 VALUES (2,20);

UPDATE test_db_3.one_pk_no_uk_1 SET val=30;
UPDATE test_db_3.one_pk_no_uk_2 SET val=30;

DELETE FROM test_db_3.one_pk_no_uk_1;
DELETE FROM test_db_3.one_pk_no_uk_2;

INSERT INTO test_db_4.one_pk_no_uk_1 VALUES (1,2);
INSERT INTO test_db_4.one_pk_no_uk_2 VALUES (2,20);

UPDATE test_db_4.one_pk_no_uk_1 SET val=30;
UPDATE test_db_4.one_pk_no_uk_2 SET val=30;

DELETE FROM test_db_4.one_pk_no_uk_1;
DELETE FROM test_db_4.one_pk_no_uk_2;

INSERT INTO test_db_5.one_pk_no_uk_1 VALUES (1,2);
INSERT INTO test_db_5.one_pk_no_uk_2 VALUES (2,20);

UPDATE test_db_5.one_pk_no_uk_1 SET val=30;
UPDATE test_db_5.one_pk_no_uk_2 SET val=30;

DELETE FROM test_db_5.one_pk_no_uk_1;
DELETE FROM test_db_5.one_pk_no_uk_2;

INSERT INTO other_test_db_1.one_pk_no_uk_1 VALUES (1,2);
INSERT INTO other_test_db_1.one_pk_no_uk_2 VALUES (2,20);

UPDATE other_test_db_1.one_pk_no_uk_1 SET val=30;
UPDATE other_test_db_1.one_pk_no_uk_2 SET val=30;

DELETE FROM other_test_db_1.one_pk_no_uk_1;
DELETE FROM other_test_db_1.one_pk_no_uk_2;