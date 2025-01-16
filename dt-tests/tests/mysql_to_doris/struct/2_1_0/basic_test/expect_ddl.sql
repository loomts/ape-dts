test_db_1
CREATE DATABASE `test_db_1`

test_db_1.one_pk_no_uk
CREATE TABLE `one_pk_no_uk` (
  `f_0` TINYINT NOT NULL,
  `f_1` SMALLINT NULL,
  `f_2` INT NULL,
  `f_3` INT NULL,
  `f_4` BIGINT NULL,
  `f_5` DECIMAL(10, 4) NULL,
  `f_6` FLOAT NULL,
  `f_7` DOUBLE NULL,
  `f_8` BIGINT NULL,
  `f_9` DATETIME(6) NULL,
  `f_10` VARCHAR(255) NULL,
  `f_11` DATE NULL,
  `f_12` INT NULL,
  `f_13` DATETIME(6) NULL,
  `f_14` CHARACTER NULL,
  `f_15` VARCHAR(255) NULL,
  `f_16` TEXT NULL,
  `f_17` TEXT NULL,
  `f_18` TEXT NULL,
  `f_19` TEXT NULL,
  `f_20` TEXT NULL,
  `f_21` TEXT NULL,
  `f_22` TEXT NULL,
  `f_23` TEXT NULL,
  `f_24` TEXT NULL,
  `f_25` TEXT NULL,
  `f_26` VARCHAR(255) NULL,
  `f_27` VARCHAR(255) NULL,
  `f_28` JSON NULL
) ENGINE=OLAP
UNIQUE KEY(`f_0`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`f_0`) BUCKETS 10
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