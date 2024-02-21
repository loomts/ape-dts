INSERT INTO test_db_1.tb_col_ansi VALUES(1, 'abc');
INSERT INTO test_db_1.tb_col_ansi VALUES(2, 'efg');
INSERT INTO test_db_1.tb_col_ansi VALUES(3, NULL);
UPDATE test_db_1.tb_col_ansi set value = 'efg' WHERE pk = 3;
UPDATE test_db_1.tb_col_ansi set value = 'abc' WHERE pk = 2;
UPDATE test_db_1.tb_col_ansi set value = NULL WHERE pk = 1;
DELETE FROM test_db_1.tb_col_ansi;

INSERT INTO test_db_1.tb_col_gbk VALUES(1, 'abc');
INSERT INTO test_db_1.tb_col_gbk VALUES(2, '中文');
INSERT INTO test_db_1.tb_col_gbk VALUES(3, NULL);
UPDATE test_db_1.tb_col_gbk set value = '中文' WHERE pk = 3;
UPDATE test_db_1.tb_col_gbk set value = 'abc' WHERE pk = 2;
UPDATE test_db_1.tb_col_gbk set value = NULL WHERE pk = 1;
DELETE FROM test_db_1.tb_col_gbk;

INSERT INTO test_db_1.tb_col_gb18030 VALUES(1, 'abc');
INSERT INTO test_db_1.tb_col_gb18030 VALUES(2, '中文');
INSERT INTO test_db_1.tb_col_gb18030 VALUES(3, NULL);
UPDATE test_db_1.tb_col_gb18030 set value = '中文' WHERE pk = 3;
UPDATE test_db_1.tb_col_gb18030 set value = 'abc' WHERE pk = 2;
UPDATE test_db_1.tb_col_gb18030 set value = NULL WHERE pk = 1;
DELETE FROM test_db_1.tb_col_gb18030;

INSERT INTO test_db_1.tb_col_gb2312 VALUES(1, 'abc');
INSERT INTO test_db_1.tb_col_gb2312 VALUES(2, '中文');
INSERT INTO test_db_1.tb_col_gb2312 VALUES(3, NULL);
UPDATE test_db_1.tb_col_gb2312 set value = '中文' WHERE pk = 3;
UPDATE test_db_1.tb_col_gb2312 set value = 'abc' WHERE pk = 2;
UPDATE test_db_1.tb_col_gb2312 set value = NULL WHERE pk = 1;
DELETE FROM test_db_1.tb_col_gb2312;

INSERT INTO test_db_1.tb_col_big5 VALUES(1, 'abc');
INSERT INTO test_db_1.tb_col_big5 VALUES(2, '中文');
INSERT INTO test_db_1.tb_col_big5 VALUES(3, NULL);
UPDATE test_db_1.tb_col_big5 set value = '中文' WHERE pk = 3;
UPDATE test_db_1.tb_col_big5 set value = 'abc' WHERE pk = 2;
UPDATE test_db_1.tb_col_big5 set value = NULL WHERE pk = 1;
DELETE FROM test_db_1.tb_col_big5;

INSERT INTO test_db_1.tb_col_ujis VALUES(1, 'abc');
INSERT INTO test_db_1.tb_col_ujis VALUES(2, 'わたし');
INSERT INTO test_db_1.tb_col_ujis VALUES(3, NULL);
UPDATE test_db_1.tb_col_ujis set value = 'わたし' WHERE pk = 3;
UPDATE test_db_1.tb_col_ujis set value = 'abc' WHERE pk = 2;
UPDATE test_db_1.tb_col_ujis set value = NULL WHERE pk = 1;
DELETE FROM test_db_1.tb_col_ujis;

INSERT INTO test_db_1.tb_col_euckr VALUES(1, 'abc');
INSERT INTO test_db_1.tb_col_euckr VALUES(2, '대한민국');
INSERT INTO test_db_1.tb_col_euckr VALUES(3, NULL);
UPDATE test_db_1.tb_col_euckr set value = '대한민국' WHERE pk = 3;
UPDATE test_db_1.tb_col_euckr set value = 'abc' WHERE pk = 2;
UPDATE test_db_1.tb_col_euckr set value = NULL WHERE pk = 1;
DELETE FROM test_db_1.tb_col_euckr;

INSERT INTO test_db_1.tb_col_utf8 VALUES(1, 'abc');
INSERT INTO test_db_1.tb_col_utf8 VALUES(2, '中文');
INSERT INTO test_db_1.tb_col_utf8 VALUES(3, 'わたし');
INSERT INTO test_db_1.tb_col_utf8 VALUES(4, '대한민국');
INSERT INTO test_db_1.tb_col_utf8 VALUES(5, NULL);
UPDATE test_db_1.tb_col_utf8 set value = '대한민국' WHERE pk = 5;
UPDATE test_db_1.tb_col_utf8 set value = 'わたし' WHERE pk = 4;
UPDATE test_db_1.tb_col_utf8 set value = '中文' WHERE pk = 3;
UPDATE test_db_1.tb_col_utf8 set value = 'abc' WHERE pk = 2;
UPDATE test_db_1.tb_col_utf8 set value = NULL WHERE pk = 1;
DELETE FROM test_db_1.tb_col_utf8;

INSERT INTO test_db_1.tb_col_utf8mb4 VALUES(1, 'abc');
INSERT INTO test_db_1.tb_col_utf8mb4 VALUES(2, '中文');
INSERT INTO test_db_1.tb_col_utf8mb4 VALUES(3, '😀');
INSERT INTO test_db_1.tb_col_utf8mb4 VALUES(4, NULL);
UPDATE test_db_1.tb_col_utf8mb4 set value = '😀' WHERE pk = 4;
UPDATE test_db_1.tb_col_utf8mb4 set value = '中文' WHERE pk = 3;
UPDATE test_db_1.tb_col_utf8mb4 set value = 'abc' WHERE pk = 2;
UPDATE test_db_1.tb_col_utf8mb4 set value = NULL WHERE pk = 1;
DELETE FROM test_db_1.tb_col_utf8mb4;
