test_db_1
CREATE DATABASE `test_db_1`

test_db_1.one_pk_no_uk
CREATE TABLE `one_pk_no_uk` (
  `f_0` tinyint(4) NOT NULL COMMENT "",
  `f_1` smallint(6) NULL COMMENT "",
  `f_2` int(11) NULL COMMENT "",
  `f_3` int(11) NULL COMMENT "",
  `f_4` bigint(20) NULL COMMENT "",
  `f_5` decimal(10, 4) NULL COMMENT "",
  `f_6` float NULL COMMENT "",
  `f_7` double NULL COMMENT "",
  `f_9` datetime NULL COMMENT "",
  `f_10` varchar(1) NULL COMMENT "",
  `f_11` date NULL COMMENT "",
  `f_12` int(11) NULL COMMENT "",
  `f_13` varchar(1) NULL COMMENT "",
  `f_14` char(255) NULL COMMENT "",
  `f_15` varchar(255) NULL COMMENT "",
  `f_16` varbinary NULL COMMENT "",
  `f_17` varbinary NULL COMMENT "",
  `f_18` varchar(65533) NULL COMMENT "",
  `f_19` varchar(65533) NULL COMMENT "",
  `f_20` varchar(65533) NULL COMMENT "",
  `f_21` varchar(65533) NULL COMMENT "",
  `f_22` varbinary NULL COMMENT "",
  `f_23` varbinary NULL COMMENT "",
  `f_24` varbinary NULL COMMENT "",
  `f_25` varbinary NULL COMMENT "",
  `f_26` varchar(1) NULL COMMENT "",
  `f_27` varchar(1) NULL COMMENT "",
  `f_28` json NULL COMMENT "",
  `_ape_dts_is_deleted` smallint(6) NULL COMMENT "",
  `_ape_dts_version` bigint(20) NULL COMMENT ""
) ENGINE=OLAP 
PRIMARY KEY(`f_0`)
DISTRIBUTED BY HASH(`f_0`)
PROPERTIES (
"replication_num" = "1",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);