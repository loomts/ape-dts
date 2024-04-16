DROP DATABASE IF EXISTS test_db_1;

CREATE DATABASE test_db_1;

CREATE TABLE test_db_1.no_pk_no_uk ( f_0 tinyint DEFAULT NULL, f_1 smallint DEFAULT NULL, f_2 mediumint DEFAULT NULL, f_3 int DEFAULT NULL, f_4 bigint DEFAULT NULL, f_5 decimal(10,4) DEFAULT NULL, f_6 float(6,2) DEFAULT NULL, f_7 double(8,3) DEFAULT NULL, f_8 bit(64) DEFAULT NULL, f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

CREATE TABLE test_db_1.one_pk_no_uk ( f_0 tinyint, f_1 smallint DEFAULT NULL, f_2 mediumint DEFAULT NULL, f_3 int DEFAULT NULL, f_4 bigint DEFAULT NULL, f_5 decimal(10,4) DEFAULT NULL, f_6 float(6,2) DEFAULT NULL, f_7 double(8,3) DEFAULT NULL, f_8 bit(64) DEFAULT NULL, f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL, PRIMARY KEY (f_0) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

CREATE TABLE test_db_1.no_pk_one_uk ( f_0 tinyint DEFAULT NULL, f_1 smallint, f_2 mediumint, f_3 int DEFAULT NULL, f_4 bigint DEFAULT NULL, f_5 decimal(10,4) DEFAULT NULL, f_6 float(6,2) DEFAULT NULL, f_7 double(8,3) DEFAULT NULL, f_8 bit(64) DEFAULT NULL, f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL, UNIQUE KEY uk_1 (f_1,f_2) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

CREATE TABLE test_db_1.no_pk_multi_uk ( f_0 tinyint DEFAULT NULL, f_1 smallint, f_2 mediumint, f_3 int, f_4 bigint, f_5 decimal(10,4), f_6 float(6,2), f_7 double(8,3), f_8 bit(64), f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL, UNIQUE KEY uk_1 (f_1,f_2), UNIQUE KEY uk_2 (f_3,f_4,f_5), UNIQUE KEY uk_3 (f_6,f_7,f_8) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4; 

CREATE TABLE test_db_1.one_pk_multi_uk ( f_0 tinyint, f_1 smallint, f_2 mediumint, f_3 int, f_4 bigint, f_5 decimal(10,4), f_6 float(6,2), f_7 double(8,3), f_8 bit(64), f_9 datetime(6) DEFAULT NULL, f_10 time(6) DEFAULT NULL, f_11 date DEFAULT NULL, f_12 year DEFAULT NULL, f_13 timestamp(6) NULL DEFAULT NULL, f_14 char(255) DEFAULT NULL, f_15 varchar(255) DEFAULT NULL, f_16 binary(255) DEFAULT NULL, f_17 varbinary(255) DEFAULT NULL, f_18 tinytext, f_19 text, f_20 mediumtext, f_21 longtext, f_22 tinyblob, f_23 blob, f_24 mediumblob, f_25 longblob, f_26 enum('x-small','small','medium','large','x-large') DEFAULT NULL, f_27 set('a','b','c','d','e') DEFAULT NULL, f_28 json DEFAULT NULL, PRIMARY KEY (f_0), UNIQUE KEY uk_1 (f_1,f_2), UNIQUE KEY uk_2 (f_3,f_4,f_5), UNIQUE KEY uk_3 (f_6,f_7,f_8) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE test_db_1.col_has_special_character_table (`p:k` tinyint, `col"1` text, `col,2` text, `col\3` text, PRIMARY KEY(`p:k`));

CREATE TABLE test_db_1.numeric_table ( f_0 tinyint, f_1 tinyint unsigned, f_2 smallint, f_3 smallint unsigned, f_4 mediumint, f_5 mediumint unsigned, f_6 int, f_7 int unsigned, f_8 bigint, f_9 bigint unsigned, PRIMARY KEY(f_0));

-- ANSI
CREATE TABLE test_db_1.tb_col_ansi(pk int, value mediumtext charset latin1, PRIMARY KEY (pk)) ENGINE=InnoDB;

-- GBK Simplified Chinese
CREATE TABLE test_db_1.tb_col_gbk(pk int, value mediumtext charset gbk, PRIMARY KEY (pk)) ENGINE=InnoDB;

-- China National Standard GB18030
CREATE TABLE test_db_1.tb_col_gb18030(pk int, value mediumtext charset gb18030, PRIMARY KEY (pk)) ENGINE=InnoDB;

-- GB2312 Simplified Chinese
CREATE TABLE test_db_1.tb_col_gb2312(pk int, value mediumtext charset gb2312, PRIMARY KEY (pk)) ENGINE=InnoDB;

-- Big5 Traditional Chinese 
CREATE TABLE test_db_1.tb_col_big5(pk int, value mediumtext charset big5, PRIMARY KEY (pk)) ENGINE=InnoDB;

-- EUC-JP Japanese
CREATE TABLE test_db_1.tb_col_ujis(pk int, value mediumtext charset ujis, PRIMARY KEY (pk)) ENGINE=InnoDB;

-- EUC-KR Korean
CREATE TABLE test_db_1.tb_col_euckr(pk int, value mediumtext charset euckr, PRIMARY KEY (pk)) ENGINE=InnoDB;

CREATE TABLE test_db_1.tb_col_utf8(pk int, value mediumtext charset utf8, PRIMARY KEY (pk)) ENGINE=InnoDB;

CREATE TABLE test_db_1.tb_col_utf8mb4(pk int, value mediumtext charset utf8mb4, PRIMARY KEY (pk)) ENGINE=InnoDB;

-- 
CREATE TABLE test_db_1.tb_col_value_has_special_char(pk int, value mediumtext charset utf8mb4, PRIMARY KEY (pk)) ENGINE=InnoDB;

-- json
CREATE TABLE test_db_1.json_test(f_0 INT AUTO_INCREMENT, f_1 JSON, PRIMARY KEY(f_0));