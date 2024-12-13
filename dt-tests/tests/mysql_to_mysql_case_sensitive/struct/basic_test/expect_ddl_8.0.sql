STRUCT_it_mysql2mysql_1
CREATE DATABASE `STRUCT_it_mysql2mysql_1` /*!40100 DEFAULT CHARACTER SET utf8mb3 */ /*!80016 DEFAULT ENCRYPTION='N' */

STRUCT_it_mysql2mysql_1.FULL_column_type
CREATE TABLE `FULL_column_type` (
  `ID` int unsigned NOT NULL AUTO_INCREMENT,
  `VARCHAR_col` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL COMMENT 'varchar_col_comment',
  `CHAR_col` char(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL COMMENT 'char_col_comment',
  `tinytext_col` tinytext CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci COMMENT 'tinytext_col_comment',
  `mediumtext_col` mediumtext CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci COMMENT 'mediumtext_col_comment',
  `longtext_col` longtext CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci COMMENT 'longtext_col_comment',
  `text_col` text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci COMMENT 'text_col_comment',
  `tinyint_col` tinyint DEFAULT '0' COMMENT 'tinyint_col_comment',
  `smallint_col` smallint DEFAULT NULL COMMENT 'smallint_col_comment',
  `mediumint_col` mediumint DEFAULT NULL COMMENT 'mediumint_col_comment',
  `int_col` int DEFAULT NULL COMMENT 'int_col_comment',
  `bigint_col` bigint DEFAULT NULL COMMENT 'bigint_col_comment',
  `float_col` float(8,2) DEFAULT NULL COMMENT 'float_col_comment',
  `double_col` double(16,4) DEFAULT NULL COMMENT 'double_col_comment',
  `bit_col` bit(64) DEFAULT NULL COMMENT 'bit_col_comment',
  `decimal_col` decimal(10,2) DEFAULT NULL COMMENT 'decimal_col_comment',
  `date_col` date DEFAULT NULL COMMENT 'date_col_comment',
  `datetime_col` datetime DEFAULT NULL COMMENT 'datetime_col_comment',
  `datetime_col2` datetime(6) DEFAULT NULL COMMENT 'datetime_col2_comment',
  `timestamp_col` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'timestamp_col_comment',
  `timestamp_co2` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'timestamp_co2_comment',
  `time_col` time DEFAULT NULL COMMENT 'time_col_comment',
  `time_col2` time(2) DEFAULT NULL COMMENT 'time_col2_comment',
  `year_col` year DEFAULT NULL COMMENT 'year_col_comment',
  `binary_col` binary(16) DEFAULT NULL COMMENT 'binary_col_comment',
  `varbinary_col` varbinary(255) DEFAULT NULL COMMENT 'varbinary_col_comment',
  `blob_col` blob COMMENT 'blob_col_comment',
  `tinyblob_col` tinyblob COMMENT 'tinyblob_col_comment',
  `mediumblob_col` mediumblob COMMENT 'mediumblob_col_comment',
  `longblob_col` longblob COMMENT 'longblob_col_comment',
  `enum_col` enum('value1','value2','value3') CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL COMMENT 'enum_col_comment',
  `set_col` set('option1','option2','option3') CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL COMMENT 'set_col_comment',
  `json_col` json DEFAULT NULL COMMENT 'json_col_comment',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3

STRUCT_it_mysql2mysql_1.FULL_index_type
CREATE TABLE `FULL_index_type` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `F_1` int DEFAULT NULL,
  `F_2` char(128) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `f_3` varchar(128) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `f_4` varchar(128) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `f_5` varchar(128) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `f_6` text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
  `f_7` text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
  `f_8` text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
  `f_9` point NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `IDX_unique_2` (`f_3`),
  UNIQUE KEY `IDX_unique_1` (`F_1`,`F_2`,`f_3`),
  SPATIAL KEY `IDX_spatial_1` (`f_9`),
  FULLTEXT KEY `IDX_full_text_1` (`f_6`,`f_7`,`f_8`),
  FULLTEXT KEY `IDX_full_text_2` (`f_8`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3

STRUCT_it_mysql2mysql_1.CONSTRAINT_table
CREATE TABLE `CONSTRAINT_table` (
  `ID` int NOT NULL AUTO_INCREMENT,
  `USERNAME` varchar(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `PASSWORD` varchar(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `email` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `age` int DEFAULT NULL,
  `gender` enum('Male','Female','Other') CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `USERNAME` (`USERNAME`),
  CONSTRAINT `CHK_age` CHECK ((`age` >= 18)),
  CONSTRAINT `CHK_email` CHECK ((`email` like _utf8mb4'%@%.%'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3