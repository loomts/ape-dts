INSERT INTO test_db_1.one_pk_no_uk VALUES (1,2,3,4,5,123456.1234,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab','cd','ef','gh','ij中文😀','kl中文😀','mn中文😀','op中文😀',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF','x-small','c', '{"key":"abc中文😀"}');
INSERT INTO test_db_1.one_pk_no_uk VALUES (2,20,30,40,50,0,4321.21,54321.321,3045,'2021-02-01 04:05:06.654321','04:05:06.654321','2012-02-01',2021,'2021-02-01 04:05:06.654321','1','2','3','4','5','6','7','8',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789','small','b', '{"key":"abc中文😀"}');
INSERT INTO test_db_1.one_pk_no_uk VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO test_db_1.one_pk_no_uk VALUES (4,2,3,4,5,6.1,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab','cd','ef','gh','ij中文😀','kl中文😀','mn中文😀','op中文😀',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF','x-small','c', '{"key":"abc中文😀"}');
INSERT INTO test_db_1.one_pk_no_uk VALUES (5,20,30,40,50,6.0,4321.21,54321.321,3045,'2021-02-01 04:05:06.654321','04:05:06.654321','2012-02-01',2021,'2021-02-01 04:05:06.654321','1','2','3','4','5','6','7','8',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789','small','b', '{"key":"abc中文😀"}');
INSERT INTO test_db_1.one_pk_no_uk VALUES (6, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO test_db_1.one_pk_no_uk VALUES (7,2,3,4,5,123456.1234,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab','cd','ef','gh','ij中文😀','kl中文😀','mn中文😀','op中文😀',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF',x'0123456789ABCDEF','x-small','c', '{"key":"abc中文😀"}');
INSERT INTO test_db_1.one_pk_no_uk VALUES (8,20,30,40,50,654321.4321,4321.21,54321.321,3045,'2021-02-01 04:05:06.654321','04:05:06.654321','2012-02-01',2021,'2021-02-01 04:05:06.654321','1','2','3','4','5','6','7','8',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789',x'ABCDEF0123456789','small','b', '{"key":"abc中文😀"}');
INSERT INTO test_db_1.one_pk_no_uk VALUES (9, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

UPDATE test_db_1.one_pk_no_uk SET f_1=20, f_2=30, f_3=40, f_4=50, f_5=654321.4321, f_6=4321.21, f_7=54321.321, f_8=3045, f_9='2021-02-01 04:05:06.654321', f_10='04:05:06.654321', f_11='2012-02-01', f_12=2021, f_13='2021-02-01 04:05:06.654321', f_14='1', f_15='2', f_16='3', f_17='4', f_18='5', f_19='6', f_20='7', f_21='8', f_22=x'ABCDEF0123456789', f_23=x'ABCDEF0123456789', f_24=x'ABCDEF0123456789', f_25=x'ABCDEF0123456789', f_26='small', f_27='b', f_28='{"key":"😀abc中文"}' WHERE f_0=1;
UPDATE test_db_1.one_pk_no_uk SET f_1=2, f_2=3, f_3=4, f_4=5, f_5=6.1, f_6=1234.12, f_7=12345.123, f_8=1893, f_9='2022-01-02 03:04:05.123456', f_10='03:04:05.123456', f_11='2022-01-02', f_12=2022, f_13='2022-01-02 03:04:05.123456', f_14='ab', f_15='cd', f_16='ef', f_17='gh', f_18='ij中文😀', f_19='kl中文😀', f_20='mn中文😀', f_21='op中文😀', f_22=x'0123456789ABCDEF', f_23=x'0123456789ABCDEF', f_24=x'0123456789ABCDEF', f_25=x'0123456789ABCDEF', f_26='x-small', f_27='c', f_28='{"key":"😀abc中文"}' WHERE f_0=2;
UPDATE test_db_1.one_pk_no_uk SET f_1=2, f_2=3, f_3=4, f_4=5, f_5=6.0, f_6=1234.12, f_7=12345.123, f_8=1893, f_9='2022-01-02 03:04:05.123456', f_10='03:04:05.123456', f_11='2022-01-02', f_12=2022, f_13='2022-01-02 03:04:05.123456', f_14='ab', f_15='cd', f_16='ef', f_17='gh', f_18='ij中文😀', f_19='kl中文😀', f_20='mn中文😀', f_21='op中文😀', f_22=x'0123456789ABCDEF', f_23=x'0123456789ABCDEF', f_24=x'0123456789ABCDEF', f_25=x'0123456789ABCDEF', f_26='x-small', f_27='c', f_28='{"key":"😀abc中文"}' WHERE f_0=3;
UPDATE test_db_1.one_pk_no_uk SET f_1=2, f_2=3, f_3=4, f_4=5, f_5=0, f_6=1234.12, f_7=12345.123, f_8=1893, f_9='2022-01-02 03:04:05.123456', f_10='03:04:05.123456', f_11='2022-01-02', f_12=2022, f_13='2022-01-02 03:04:05.123456', f_14='ab', f_15='cd', f_16='ef', f_17='gh', f_18='ij中文😀', f_19='kl中文😀', f_20='mn中文😀', f_21='op中文😀', f_22=x'0123456789ABCDEF', f_23=x'0123456789ABCDEF', f_24=x'0123456789ABCDEF', f_25=x'0123456789ABCDEF', f_26='x-small', f_27='c', f_28='{"key":"😀abc中文"}' WHERE f_0=4;
UPDATE test_db_1.one_pk_no_uk SET f_1=NULL, f_2=NULL, f_3=NULL, f_4=NULL, f_5=NULL, f_6=NULL, f_7=NULL, f_8=NULL, f_9=NULL, f_10=NULL, f_11=NULL, f_12=NULL, f_13=NULL, f_14=NULL, f_15=NULL, f_16=NULL, f_17=NULL, f_18=NULL, f_19=NULL, f_20=NULL, f_21=NULL, f_22=NULL, f_23=NULL, f_24=NULL, f_25=NULL, f_26=NULL, f_27=NULL, f_28=NULL WHERE f_0=5;

DELETE FROM test_db_1.one_pk_no_uk;

INSERT INTO test_db_1.timestamp_table VALUES (1, '2022-01-02 03:04:05.123456', '2022-01-02 03:04:05.123');
INSERT INTO test_db_1.timestamp_table VALUES (2, '2022-01-02 03:04:05.1', '2022-01-02 03:04:05.1');
INSERT INTO test_db_1.timestamp_table VALUES (3, '2022-01-02 03:04:05.0', '2022-01-02 03:04:05');

UPDATE test_db_1.timestamp_table SET f_1 = '2022-01-02 03:04:05.1', f_2 = '2022-01-02 03:04:05.1' WHERE f_0 = 1;
UPDATE test_db_1.timestamp_table SET f_1 = '2022-01-02 03:04:05.0', f_2 = '2022-01-02 03:04:05' WHERE f_0 = 2;
UPDATE test_db_1.timestamp_table SET f_1 = '2022-01-02 03:04:05.123456', f_2 = '2022-01-02 03:04:05.123' WHERE f_0 = 3;

DELETE FROM test_db_1.timestamp_table;

-- min for each col
INSERT INTO test_db_1.numeric_table VALUES(-128, 0, -32768, 0, -8388608, 0, -2147483648, 0, -9223372036854775808, 0);
-- max for each col
INSERT INTO test_db_1.numeric_table VALUES(127, 255, 32767, 65535, 8388607, 16777215, 2147483647, 4294967295, 9223372036854775807, 18446744073709551615);
-- zero
INSERT INTO test_db_1.numeric_table VALUES(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

UPDATE test_db_1.numeric_table SET f_1=255, f_2=32767, f_3=65535, f_4=8388607, f_5=16777215, f_6=2147483647, f_7=4294967295, f_8=9223372036854775807, f_9=18446744073709551615 WHERE f_0=-128;
UPDATE test_db_1.numeric_table SET f_1=0, f_2=0, f_3=0, f_4=0, f_5=0, f_6=0, f_7=0, f_8=0, f_9=0 WHERE f_0=127;
UPDATE test_db_1.numeric_table SET f_1=0, f_2=-32768, f_3=0, f_4=-8388608, f_5=0, f_6=-2147483648, f_7=0, f_8=-9223372036854775808, f_9=0 WHERE f_0=0;

DELETE FROM test_db_1.numeric_table;

-- for value 0 (decimal(47,25)), mysql: 0.0000000000000000000000000, foxlake: 0E-25
-- INSERT INTO test_db_1.big_decimal_table VALUES(0, 0);
INSERT INTO test_db_1.big_decimal_table VALUES(1, -0.9);
INSERT INTO test_db_1.big_decimal_table VALUES(2, -9000000000000000000000.0000000000000000000000009);
INSERT INTO test_db_1.big_decimal_table VALUES(3, 0.9);
INSERT INTO test_db_1.big_decimal_table VALUES(4, 9000000000000000000000.0000000000000000000000009);

UPDATE test_db_1.big_decimal_table SET f_1 = -9000000000000000000000.0000000000000000000000009 WHERE f_0 = 1;
UPDATE test_db_1.big_decimal_table SET f_1 = 0.9 WHERE f_0 = 2;
UPDATE test_db_1.big_decimal_table SET f_1 = 9000000000000000000000.0000000000000000000000009 WHERE f_0 = 3;
UPDATE test_db_1.big_decimal_table SET f_1 = -0.9 WHERE f_0 = 4;

DELETE FROM test_db_1.big_decimal_table;