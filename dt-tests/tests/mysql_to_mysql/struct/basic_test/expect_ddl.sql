struct_it_mysql2mysql_1.table_test
CREATE TABLE `table_test` (
  `id` int NOT NULL,
  `text` varchar(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL COMMENT 'col comment test',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COMMENT='table comment test'

struct_it_mysql2mysql_1.full_column_type
CREATE TABLE `full_column_type` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `varchar_col` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `char_col` char(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `text_col` text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
  `tinyint_col` tinyint DEFAULT '0',
  `smallint_col` smallint DEFAULT NULL,
  `mediumint_col` mediumint DEFAULT NULL,
  `int_col` int DEFAULT NULL,
  `bigint_col` bigint DEFAULT NULL,
  `float_col` float(8,2) DEFAULT NULL,
  `double_col` double(16,4) DEFAULT NULL,
  `decimal_col` decimal(10,2) DEFAULT NULL,
  `date_col` date DEFAULT NULL,
  `datetime_col` datetime DEFAULT NULL,
  `timestamp_col` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `time_col` time DEFAULT NULL,
  `year_col` year DEFAULT NULL,
  `binary_col` binary(16) DEFAULT NULL,
  `varbinary_col` varbinary(255) DEFAULT NULL,
  `blob_col` blob,
  `tinyblob_col` tinyblob,
  `mediumblob_col` mediumblob,
  `longblob_col` longblob,
  `enum_col` enum('value1','value2','value3') CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `set_col` set('option1','option2','option3') CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3

struct_it_mysql2mysql_1.full_index_type
CREATE TABLE `full_index_type` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `unique_col` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `index_col` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `fulltext_col` text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
  `spatial_col` point NOT NULL,
  `simple_index_col` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `composite_index_col1` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `composite_index_col2` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `composite_index_col3` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_index` (`unique_col`) USING BTREE,
  KEY `simple_index` (`simple_index_col`) USING BTREE,
  KEY `index_index` (`index_col`) USING BTREE,
  KEY `composite_index` (`composite_index_col1`,`composite_index_col2`,`composite_index_col3`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3

struct_it_mysql2mysql_1.constraint_table
CREATE TABLE `constraint_table` (
  `id` int NOT NULL AUTO_INCREMENT,
  `username` varchar(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `password` varchar(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `email` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `age` int DEFAULT NULL,
  `gender` enum('Male','Female','Other') CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`) USING BTREE,
  CONSTRAINT `chk_age` CHECK ((`age` >= 18)),
  CONSTRAINT `chk_email` CHECK ((`email` like _utf8mb4'%@%.%'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3

