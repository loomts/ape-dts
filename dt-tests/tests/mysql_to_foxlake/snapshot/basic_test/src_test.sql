INSERT INTO test_db_1.one_pk_no_uk VALUES (1,2,3,4,5,123456.1234,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab中文','cd中文','ef','gh', 'ij中文😀', 'kl中文😀', 'mn中文😀', 'op中文😀',0x0123456789ABCDEF, 0x0123456789ABCDEF, 0x0123456789ABCDEF, 0x0123456789ABCDEF,'x-small','c','{"key":"abc中文😀"}');
-- decimal: 123456.1
INSERT INTO test_db_1.one_pk_no_uk VALUES (2,2,3,4,5,6.1,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab中文','cd中文','ef','gh', 'ij中文😀', 'kl中文😀', 'mn中文😀', 'op中文😀',0x0123456789ABCDEF, 0x0123456789ABCDEF, 0x0123456789ABCDEF, 0x0123456789ABCDEF,'x-small','c','{"key":"abc中文😀"}');
-- decimal: 6.0
INSERT INTO test_db_1.one_pk_no_uk VALUES (3,2,3,4,5,6.0,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab中文','cd中文','ef','gh', 'ij中文😀', 'kl中文😀', 'mn中文😀', 'op中文😀',0x0123456789ABCDEF, 0x0123456789ABCDEF, 0x0123456789ABCDEF, 0x0123456789ABCDEF,'x-small','c','{"key":"abc中文😀"}');
-- decimal: 0
INSERT INTO test_db_1.one_pk_no_uk VALUES (4,2,3,4,5,0,1234.12,12345.123,1893,'2022-01-02 03:04:05.123456','03:04:05.123456','2022-01-02',2022,'2022-01-02 03:04:05.123456','ab中文','cd中文','ef','gh', 'ij中文😀', 'kl中文😀', 'mn中文😀', 'op中文😀',0x0123456789ABCDEF, 0x0123456789ABCDEF, 0x0123456789ABCDEF, 0x0123456789ABCDEF,'x-small','c','{"key":"abc中文😀"}');
INSERT INTO test_db_1.one_pk_no_uk VALUES (5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- min for each col
INSERT INTO test_db_1.numeric_table VALUES(-128, 0, -32768, 0, -8388608, 0, -2147483648, 0, -9223372036854775808, 0);
-- max for each col
INSERT INTO test_db_1.numeric_table VALUES(127, 255, 32767, 65535, 8388607, 16777215, 2147483647, 4294967295, 9223372036854775807, 18446744073709551615);
-- zero
INSERT INTO test_db_1.numeric_table VALUES(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

-- for value 0 (decimal(47,25)), mysql: 0.0000000000000000000000000, foxlake: 0E-25
INSERT INTO test_db_1.big_decimal_table VALUES(0, 0);
INSERT INTO test_db_1.big_decimal_table VALUES(1, -0.9);
INSERT INTO test_db_1.big_decimal_table VALUES(2, -9000000000000000000000.0000000000000000000000009);
INSERT INTO test_db_1.big_decimal_table VALUES(3, 0.9);
INSERT INTO test_db_1.big_decimal_table VALUES(4, 9000000000000000000000.0000000000000000000000009);

INSERT INTO test_db_1.date_time_table VALUES(1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
-- min for each col
INSERT INTO test_db_1.date_time_table VALUES(2, '1000-01-01 00:00:00', '1000-01-01 00:00:00.000000', '-838:59:59', '-838:59:59.000000', '1970-01-01 00:00:01', '1970-01-01 00:00:01.000000', '1000-01-01', 1901);
-- max for each col
INSERT INTO test_db_1.date_time_table VALUES(3, '9999-12-31 23:59:59', '9999-12-31 23:59:59.499999', '838:59:59', '838:59:59.000000', '2038-01-19 03:14:07', '2038-01-19 03:14:07.499999', '9999-12-31', 2155);
-- zero
INSERT INTO test_db_1.date_time_table VALUES(4, '1000-01-01 00:00:00', '1000-01-01 00:00:00.000000', '00:00:00', '00:00:00.000000', '1970-01-01 00:00:01', '1970-01-01 00:00:01.000000', '1000-01-01', 1901);
