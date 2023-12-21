struct_it_mysql2mysql_1
CREATE DATABASE `struct_it_mysql2mysql_1` /*!40100 DEFAULT CHARACTER SET utf8 */

struct_it_mysql2mysql_1.table_test
CREATE TABLE `table_test` (
  `id` int(11) NOT NULL,
  `text` varchar(10) DEFAULT NULL COMMENT 'col comment test',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='table comment test'

struct_it_mysql2mysql_1.full_column_type
CREATE TABLE `full_column_type` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `varchar_col` varchar(255) NOT NULL,
  `char_col` char(10) DEFAULT NULL,
  `text_col` text,
  `tinyint_col` tinyint(4) DEFAULT '0',
  `smallint_col` smallint(6) DEFAULT NULL,
  `mediumint_col` mediumint(9) DEFAULT NULL,
  `int_col` int(11) DEFAULT NULL,
  `bigint_col` bigint(20) DEFAULT NULL,
  `float_col` float(8,2) DEFAULT NULL,
  `double_col` double(16,4) DEFAULT NULL,
  `decimal_col` decimal(10,2) DEFAULT NULL,
  `date_col` date DEFAULT NULL,
  `datetime_col` datetime DEFAULT NULL,
  `timestamp_col` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `time_col` time DEFAULT NULL,
  `year_col` year(4) DEFAULT NULL,
  `binary_col` binary(16) DEFAULT NULL,
  `varbinary_col` varbinary(255) DEFAULT NULL,
  `blob_col` blob,
  `tinyblob_col` tinyblob,
  `mediumblob_col` mediumblob,
  `longblob_col` longblob,
  `enum_col` enum('value1','value2','value3') DEFAULT NULL,
  `set_col` set('option1','option2','option3') DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

struct_it_mysql2mysql_1.full_index_type
CREATE TABLE `full_index_type` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `unique_col` varchar(255) NOT NULL,
  `index_col` varchar(255) DEFAULT NULL,
  `fulltext_col` text,
  `spatial_col` point NOT NULL,
  `simple_index_col` varchar(255) DEFAULT NULL,
  `composite_index_col1` varchar(255) DEFAULT NULL,
  `composite_index_col2` varchar(255) DEFAULT NULL,
  `composite_index_col3` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_index` (`unique_col`) USING BTREE,
  KEY `composite_index` (`composite_index_col1`,`composite_index_col2`,`composite_index_col3`) USING BTREE,
  KEY `index_index` (`index_col`) USING BTREE,
  KEY `simple_index` (`simple_index_col`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8

struct_it_mysql2mysql_1.constraint_table
CREATE TABLE `constraint_table` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(50) NOT NULL,
  `password` varchar(50) NOT NULL,
  `email` varchar(100) NOT NULL,
  `age` int(11) DEFAULT NULL,
  `gender` enum('Male','Female','Other') NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8
