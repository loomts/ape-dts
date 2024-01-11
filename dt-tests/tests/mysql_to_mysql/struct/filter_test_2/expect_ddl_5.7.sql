struct_it_mysql2mysql_1
CREATE DATABASE `struct_it_mysql2mysql_1` /*!40100 DEFAULT CHARACTER SET utf8 */

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
  UNIQUE KEY `unique_index` (`unique_col`),
  KEY `composite_index` (`composite_index_col1`,`composite_index_col2`,`composite_index_col3`),
  KEY `simple_index` (`simple_index_col`),
  KEY `index_index` (`index_col`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

struct_it_mysql2mysql_1.constraint_table
CREATE TABLE `constraint_table` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(50) NOT NULL,
  `password` varchar(50) NOT NULL,
  `email` varchar(100) NOT NULL,
  `age` int(11) DEFAULT NULL,
  `gender` enum('Male','Female','Other') NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

struct_it_mysql2mysql_1.foreign_key_parent
CREATE TABLE `foreign_key_parent` (
  `pk` int(11) NOT NULL,
  `parent_col_1` int(11) DEFAULT NULL,
  `parent_col_2` int(11) DEFAULT NULL,
  PRIMARY KEY (`pk`),
  UNIQUE KEY `parent_col_1` (`parent_col_1`),
  UNIQUE KEY `parent_col_2` (`parent_col_2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

struct_it_mysql2mysql_1.foreign_key_child
CREATE TABLE `foreign_key_child` (
  `pk` int(11) NOT NULL,
  `child_col_1` int(11) DEFAULT NULL,
  `child_col_2` int(11) DEFAULT NULL,
  PRIMARY KEY (`pk`),
  UNIQUE KEY `child_col_1` (`child_col_1`),
  UNIQUE KEY `child_col_2` (`child_col_2`),
  CONSTRAINT `fk_test_1` FOREIGN KEY (`child_col_1`) REFERENCES `foreign_key_parent` (`parent_col_1`),
  CONSTRAINT `fk_test_2` FOREIGN KEY (`child_col_2`) REFERENCES `foreign_key_parent` (`parent_col_2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8