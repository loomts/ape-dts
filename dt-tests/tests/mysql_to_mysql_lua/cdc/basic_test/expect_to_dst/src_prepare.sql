DROP DATABASE IF EXISTS lua_test_expect;

CREATE DATABASE lua_test_expect;

CREATE TABLE `lua_test_expect`.`add_column_test` ( `id` int(11) NOT NULL, `f_1` int(11) DEFAULT NULL, `f_2` int(11) DEFAULT NULL, PRIMARY KEY (`id`) ) ENGINE = InnoDB CHARSET = utf8;  
CREATE TABLE `lua_test_expect`.`change_column_value_test` ( `id` int(11) NOT NULL, `f_1` int(11) DEFAULT NULL, PRIMARY KEY (`id`) ) ENGINE = InnoDB CHARSET = utf8;  
CREATE TABLE `lua_test_expect`.`drop_column_test` ( `id` int(11) NOT NULL, `f_1` int(11) DEFAULT NULL, PRIMARY KEY (`id`) ) ENGINE = InnoDB CHARSET = utf8;  
CREATE TABLE `lua_test_expect`.`change_column_name_test` ( `id` int(11) NOT NULL, `f_1_1` int(11) DEFAULT NULL, PRIMARY KEY (`id`) ) ENGINE = InnoDB CHARSET = utf8; 
CREATE TABLE `lua_test_expect`.`change_table_name_test_dst` ( `id` int(11) NOT NULL, `f_1` int(11) DEFAULT NULL, PRIMARY KEY (`id`) ) ENGINE = InnoDB CHARSET = utf8;  
CREATE TABLE `lua_test_expect`.`filter_row_test` ( `id` int(11) NOT NULL, `f_1` int(11) DEFAULT NULL, PRIMARY KEY (`id`) ) ENGINE = InnoDB CHARSET = utf8;

CREATE TABLE `lua_test_expect`.`change_string_column_value_test` ( `id` int(11) NOT NULL, `f_1` varchar(255) DEFAULT NULL, `f_2` char(255) DEFAULT NULL, `f_3` tinytext DEFAULT NULL, `f_4` mediumtext DEFAULT NULL, `f_5` longtext DEFAULT NULL, `f_6` text DEFAULT NULL, PRIMARY KEY (`id`) ) ENGINE = InnoDB CHARSET = utf8mb4;

CREATE TABLE `lua_test_expect`.`filter_blob_column_value_test` ( `id` int(11) NOT NULL, `f_1` varbinary(255) DEFAULT NULL, `f_2` binary(255) DEFAULT NULL, `f_3` tinyblob DEFAULT NULL, `f_4` blob DEFAULT NULL, `f_5` mediumblob DEFAULT NULL, `f_6` longblob DEFAULT NULL, PRIMARY KEY (`id`) ) ENGINE = InnoDB CHARSET = utf8mb4;