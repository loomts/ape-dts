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

