test_db_1
CREATE DATABASE `test_db_1`

test_db_1.one_pk_no_uk
CREATE TABLE `one_pk_no_uk` (
  `f_0` tinyint(4) NOT NULL COMMENT "",
  `f_1` smallint(6) NULL COMMENT "",
  `f_2` int(11) NULL COMMENT "",
  `f_3` int(11) NULL COMMENT "",
  `f_4` bigint(20) NULL COMMENT "",
  `f_5` decimal64(10, 4) NULL COMMENT "",
  `f_6` float NULL COMMENT "",
  `f_7` double NULL COMMENT "",
  `f_8` bigint(20) NULL COMMENT "",
  `f_9` datetime NULL COMMENT "",
  `f_10` varchar(255) NULL COMMENT "",
  `f_11` date NULL COMMENT "",
  `f_12` int(11) NULL COMMENT "",
  `f_13` datetime NULL COMMENT "",
  `f_14` char(255) NULL COMMENT "",
  `f_15` varchar(255) NULL COMMENT "",
  `f_18` varchar(65533) NULL COMMENT "",
  `f_19` varchar(65533) NULL COMMENT "",
  `f_20` varchar(65533) NULL COMMENT "",
  `f_21` varchar(65533) NULL COMMENT "",
  `f_26` varchar(255) NULL COMMENT "",
  `f_27` varchar(255) NULL COMMENT "",
  `f_28` json NULL COMMENT "",
  `_ape_dts_is_deleted` boolean NULL COMMENT "",
  `_ape_dts_timestamp` bigint(20) NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`f_0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`f_0`) BUCKETS 2 
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"storage_format" = "DEFAULT",
"enable_persistent_index" = "false",
"compression" = "LZ4"
);