DROP DATABASE IF EXISTS test_db_1;
CREATE DATABASE test_db_1;

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

CREATE TABLE test_db_1.tb_col_utf8mb4(pk int, value mediumtext charset utf8mb4, PRIMARY KEY (pk)) ENGINE=InnoDB ;

