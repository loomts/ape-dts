test_db_1
CREATE DATABASE `test_db_1`

dst_test_db_2
CREATE DATABASE `dst_test_db_2`

test_db_1.full_column_type
CREATE TABLE `full_column_type` (
  `id` INT NOT NULL,
  `char_col` TEXT NULL,
  `char_col_2` TEXT NULL,
  `character_col` TEXT NULL,
  `character_col_2` TEXT NULL,
  `varchar_col` TEXT NULL,
  `varchar_col_2` TEXT NULL,
  `character_varying_col` TEXT NULL,
  `character_varying_col_2` TEXT NULL,
  `bpchar_col` TEXT NULL,
  `bpchar_col_2` TEXT NULL,
  `text_col` TEXT NULL,
  `real_col` FLOAT NULL,
  `float4_col` FLOAT NULL,
  `double_precision_col` DOUBLE NULL,
  `float8_col` DOUBLE NULL,
  `numeric_col` DECIMAL(38, 9) NULL,
  `numeric_col_2` DECIMAL(38, 9) NULL,
  `decimal_col` DECIMAL(38, 9) NULL,
  `decimal_col_2` DECIMAL(38, 9) NULL,
  `smallint_col` SMALLINT NULL,
  `int2_col` SMALLINT NULL,
  `smallserial_col` SMALLINT NULL,
  `serial2_col` SMALLINT NULL,
  `integer_col` INT NULL,
  `int_col` INT NULL,
  `int4_col` INT NULL,
  `serial_col` INT NULL,
  `serial4_col` INT NULL,
  `bigint_col` BIGINT NULL,
  `int8_col` BIGINT NULL,
  `bigserial_col` BIGINT NULL,
  `serial8_col` BIGINT NULL,
  `bit_col` TEXT NULL,
  `bit_col_2` TEXT NULL,
  `bit_varying_col` TEXT NULL,
  `bit_varying_col_2` TEXT NULL,
  `varbit_col` TEXT NULL,
  `varbit_col_2` TEXT NULL,
  `time_col` VARCHAR(255) NULL,
  `time_col_2` VARCHAR(255) NULL,
  `time_col_3` VARCHAR(255) NULL,
  `time_col_4` VARCHAR(255) NULL,
  `timez_col` VARCHAR(255) NULL,
  `timez_col_2` VARCHAR(255) NULL,
  `timez_col_3` VARCHAR(255) NULL,
  `timez_col_4` VARCHAR(255) NULL,
  `timestamp_col` DATETIME(6) NULL,
  `timestamp_col_2` DATETIME(6) NULL,
  `timestamp_col_3` DATETIME(6) NULL,
  `timestamp_col_4` DATETIME(6) NULL,
  `timestampz_col` DATETIME(6) NULL,
  `timestampz_col_2` DATETIME(6) NULL,
  `timestampz_col_3` DATETIME(6) NULL,
  `timestampz_col_4` DATETIME(6) NULL,
  `date_col` DATE NULL,
  `bytea_col` TEXT NULL,
  `boolean_col` BOOLEAN NULL,
  `bool_col` BOOLEAN NULL,
  `json_col` JSON NULL,
  `jsonb_col` JSON NULL,
  `interval_col` VARCHAR(255) NULL,
  `interval_col_2` VARCHAR(255) NULL,
  `array_float4_col` TEXT NULL,
  `array_float8_col` TEXT NULL,
  `array_int2_col` TEXT NULL,
  `array_int4_col` TEXT NULL,
  `array_int8_col` TEXT NULL,
  `array_int8_col_2` TEXT NULL,
  `array_text_col` TEXT NULL,
  `array_boolean_col` TEXT NULL,
  `array_boolean_col_2` TEXT NULL,
  `array_date_col` TEXT NULL,
  `array_timestamp_col` TEXT NULL,
  `array_timestamp_col_2` TEXT NULL,
  `array_timestamptz_col` TEXT NULL,
  `array_timestamptz_col_2` TEXT NULL,
  `box_col` TEXT NULL,
  `cidr_col` TEXT NULL,
  `circle_col` TEXT NULL,
  `inet_col` TEXT NULL,
  `line_col` TEXT NULL,
  `lseg_col` TEXT NULL,
  `macaddr_col` TEXT NULL,
  `macaddr8_col` TEXT NULL,
  `money_col` TEXT NULL,
  `path_col` TEXT NULL,
  `pg_lsn_col` TEXT NULL,
  `pg_snapshot_col` TEXT NULL,
  `polygon_col` TEXT NULL,
  `point_col` TEXT NULL,
  `tsquery_col` TEXT NULL,
  `tsvector_col` TEXT NULL,
  `txid_snapshot_col` TEXT NULL,
  `uuid_col` TEXT NULL,
  `xml_col` TEXT NULL
) ENGINE=OLAP
UNIQUE KEY(`id`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);

test_db_1.array_table
CREATE TABLE `array_table` (
  `pk` INT NOT NULL,
  `int_array` TEXT NULL,
  `bigint_array` TEXT NULL,
  `text_array` TEXT NULL,
  `char_array` TEXT NULL,
  `varchar_array` TEXT NULL,
  `date_array` TEXT NULL,
  `numeric_array` TEXT NULL,
  `varnumeric_array` TEXT NULL,
  `inet_array` TEXT NULL,
  `cidr_array` TEXT NULL,
  `macaddr_array` TEXT NULL,
  `tsrange_array` TEXT NULL,
  `tstzrange_array` TEXT NULL,
  `daterange_array` TEXT NULL,
  `int4range_array` TEXT NULL,
  `numerange_array` TEXT NULL,
  `int8range_array` TEXT NULL,
  `uuid_array` TEXT NULL,
  `json_array` TEXT NULL,
  `jsonb_array` TEXT NULL,
  `oid_array` TEXT NULL
) ENGINE=OLAP
UNIQUE KEY(`pk`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`pk`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);

test_db_1.check_pk_cols_order
CREATE TABLE `check_pk_cols_order` (
  `pk_1` INT NOT NULL,
  `pk_2` INT NOT NULL,
  `pk_3` INT NOT NULL,
  `col_1` INT NULL,
  `col_2` INT NULL,
  `col_3` INT NULL,
  `col_4` INT NULL,
  `col_5` INT NULL
) ENGINE=OLAP
UNIQUE KEY(`pk_1`, `pk_2`, `pk_3`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`pk_1`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);

dst_test_db_2.router_test_1
CREATE TABLE `router_test_1` (
  `pk` INT NOT NULL,
  `col_1` INT NULL
) ENGINE=OLAP
UNIQUE KEY(`pk`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`pk`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);

dst_test_db_2.dst_router_test_2
CREATE TABLE `dst_router_test_2` (
  `pk` INT NOT NULL,
  `col_1` INT NULL
) ENGINE=OLAP
UNIQUE KEY(`pk`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`pk`) BUCKETS 10
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"min_load_replica_num" = "-1",
"is_being_synced" = "false",
"storage_medium" = "hdd",
"storage_format" = "V2",
"enable_unique_key_merge_on_write" = "true",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);