# 简介
- 结构迁移后，我们提供两种校验方式，一种是我们自带的，一种是第三方的 [liquibase](./check_liquibase.md)
- 本文介绍我们自带的

# 配置
```
[extractor]
db_type=mysql
extract_type=struct
url=mysql://root:123456@127.0.0.1:3307?ssl-mode=disabled

[sinker]
db_type=mysql
sink_type=check
url=mysql://root:123456@127.0.0.1:3308?ssl-mode=disabled
batch_size=1

[filter]
do_dbs=
ignore_dbs=
do_tbs=struct_check_test_1.*
ignore_tbs=
do_events=insert

[router]
db_map=
tb_map=
field_map=

[parallelizer]
parallel_type=rdb_check
parallel_size=1

[pipeline]
buffer_size=100
checkpoint_interval_secs=10

[runtime]
log_level=info
log4rs_file=./log4rs.yaml
log_dir=./logs
```

# 校验结果
- 以源端结构为基准，校验结果包括 miss，diff，extra（目标多出）
- 校验结果会以 sql 的方式呈现，miss.log 中包含 src_sql，extra.log 中包含 dst_sql，diff.log 中包含 src_sql 和 dst_sql

- miss.log
```
[("table.struct_check_test_1.not_match_miss", "CREATE TABLE `struct_check_test_1`.`not_match_miss` (`id` int(11) NOT NULL  ,`text` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL  , PRIMARY KEY (`id`)) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci")]
key: index.struct_check_test_1.not_match_index.i6_miss, src_sql: CREATE  INDEX `i6_miss` ON `struct_check_test_1`.`not_match_index` (`index_col`) 
key: index.struct_check_test_1.not_match_index.i5_diff_name_src, src_sql: CREATE  INDEX `i5_diff_name_src` ON `struct_check_test_1`.`not_match_index` (`index_col`) 
```

- diff.log
```
key: index.struct_check_test_1.not_match_index.i4_diff_order, src_sql: CREATE  INDEX `i4_diff_order` ON `struct_check_test_1`.`not_match_index` (`composite_index_col2`,`composite_index_col1`,`composite_index_col3`) 
key: index.struct_check_test_1.not_match_index.i4_diff_order, dst_sql: CREATE  INDEX `i4_diff_order` ON `struct_check_test_1`.`not_match_index` (`composite_index_col3`,`composite_index_col2`,`composite_index_col1`) 
key: table.struct_check_test_1.not_match_column, src_sql: CREATE TABLE `struct_check_test_1`.`not_match_column` (`id` int(10) unsigned auto_increment NOT NULL  ,`varchar_col` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL  ,`char_col` char(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL  ,`text_col` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL  ,`tinyint_col` tinyint(4) DEFAULT '0' NULL  ,`smallint_col` smallint(6) NULL  ,`mediumint_col` mediumint(9) NULL  ,`int_col` int(11) NULL  ,`bigint_col` bigint(20) NULL  ,`float_col` float(8,2) NULL  ,`double_col` double(16,4) NULL  ,`decimal_col` decimal(10,2) NULL  ,`date_col` date NULL  ,`datetime_col` datetime NULL  ,`timestamp_col` timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL  ,`time_col` time NULL  ,`year_col` year(4) NULL  ,`binary_col` binary(16) NULL  ,`varbinary_col` varbinary(255) NULL  ,`blob_col` blob NULL  ,`tinyblob_col` tinyblob NULL  ,`mediumblob_col` mediumblob NULL  ,`longblob_col` longblob NULL  ,`enum_col` enum('value1','value2','value3') CHARACTER SET utf8 COLLATE utf8_general_ci NULL  ,`set_col` set('option1','option2','option3') CHARACTER SET utf8 COLLATE utf8_general_ci NULL  , PRIMARY KEY (`id`)) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci
key: table.struct_check_test_1.not_match_column, dst_sql: CREATE TABLE `struct_check_test_1`.`not_match_column` (`id` int(10) unsigned auto_increment NOT NULL  ,`char_col` char(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL  ,`text_col` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL  ,`tinyint_col` tinyint(4) DEFAULT '0' NULL  ,`smallint_col` smallint(6) NULL  ,`mediumint_col` mediumint(9) NULL  ,`int_col` int(11) NULL  ,`bigint_col` bigint(20) NULL  ,`float_col` float(8,2) NULL  ,`double_col` double(16,4) NULL  ,`decimal_col` decimal(10,2) NULL  ,`datetime_col` datetime NULL  ,`timestamp_col` timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL  ,`time_col` time NULL  ,`year_col` year(4) NULL  ,`binary_col` binary(16) NULL  ,`varbinary_col` varbinary(255) NULL  ,`blob_col` blob NULL  ,`tinyblob_col` tinyblob NULL  ,`mediumblob_col` mediumblob NULL  ,`longblob_col` longblob NULL  ,`enum_col` enum('value1','value2','value3') CHARACTER SET utf8 COLLATE utf8_general_ci NULL  ,`set_col` set('option1','option2','option3') CHARACTER SET utf8 COLLATE utf8_general_ci NULL  , PRIMARY KEY (`id`)) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci
```

- extra.log
```
key: index.struct_check_test_1.not_match_index.i5_diff_name_dst, dst_sql: CREATE  INDEX `i5_diff_name_dst` ON `struct_check_test_1`.`not_match_index` (`index_col`) 
```