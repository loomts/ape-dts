struct_it_mysql2mysql_1
CREATE DATABASE `struct_it_mysql2mysql_1` /*!40100 DEFAULT CHARACTER SET utf8mb3 */ /*!80016 DEFAULT ENCRYPTION='N' */

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

struct_it_mysql2mysql_1.foreign_key_parent
CREATE TABLE `foreign_key_parent` (
  `pk` int NOT NULL,
  `parent_col_1` int DEFAULT NULL,
  `parent_col_2` int DEFAULT NULL,
  PRIMARY KEY (`pk`),
  UNIQUE KEY `parent_col_1` (`parent_col_1`),
  UNIQUE KEY `parent_col_2` (`parent_col_2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3

struct_it_mysql2mysql_1.foreign_key_child
CREATE TABLE `foreign_key_child` (
  `pk` int NOT NULL,
  `child_col_1` int DEFAULT NULL,
  `child_col_2` int DEFAULT NULL,
  PRIMARY KEY (`pk`),
  UNIQUE KEY `child_col_1` (`child_col_1`),
  UNIQUE KEY `child_col_2` (`child_col_2`),
  CONSTRAINT `fk_test_1` FOREIGN KEY (`child_col_1`) REFERENCES `foreign_key_parent` (`parent_col_1`),
  CONSTRAINT `fk_test_2` FOREIGN KEY (`child_col_2`) REFERENCES `foreign_key_parent` (`parent_col_2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3