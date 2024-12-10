test_db_1
CREATE DATABASE `test_db_1`

test_db_1.one_pk_no_uk
CREATE TABLE test_db_1.one_pk_no_uk
(
    `pk` Int8,
    `tinyint_col` Nullable(Int8),
    `tinyint_col_unsigned` Nullable(UInt8),
    `smallint_col` Nullable(Int16),
    `smallint_col_unsigned` Nullable(UInt16),
    `mediumint_col` Nullable(Int32),
    `mediumint_col_unsigned` Nullable(UInt32),
    `int_col` Nullable(Int32),
    `int_col_unsigned` Nullable(UInt32),
    `bigint_col` Nullable(Int64),
    `bigint_col_unsigned` Nullable(UInt64),
    `decimal_col` Nullable(Decimal(10, 4)),
    `float_col` Nullable(Float32),
    `double_col` Nullable(Float64),
    `bit_col` Nullable(UInt64),
    `datetime_col` Nullable(DateTime64(6)),
    `time_col` Nullable(String),
    `date_col` Nullable(Date32),
    `year_col` Nullable(Int32),
    `timestamp_col` Nullable(DateTime64(6)),
    `char_col` Nullable(String),
    `varchar_col` Nullable(String),
    `binary_col` Nullable(String),
    `varbinary_col` Nullable(String),
    `tinytext_col` Nullable(String),
    `text_col` Nullable(String),
    `mediumtext_col` Nullable(String),
    `longtext_col` Nullable(String),
    `tinyblob_col` Nullable(String),
    `blob_col` Nullable(String),
    `mediumblob_col` Nullable(String),
    `longblob_col` Nullable(String),
    `enum_col` Nullable(String),
    `set_col` Nullable(String),
    `json_col` Nullable(String),
    `_ape_dts_is_deleted` Int8,
    `_ape_dts_timestamp` Int64
)
ENGINE = ReplacingMergeTree(_ape_dts_timestamp)
PRIMARY KEY pk
ORDER BY pk
SETTINGS index_granularity = 8192

test_db_1.check_pk_cols_order
CREATE TABLE test_db_1.check_pk_cols_order
(
    `col_1` Nullable(Int32),
    `col_2` Nullable(Int32),
    `col_3` Nullable(Int32),
    `pk_3` Int32,
    `pk_1` Int32,
    `col_4` Nullable(Int32),
    `pk_2` Int32,
    `col_5` Nullable(Int32),
    `_ape_dts_is_deleted` Int8,
    `_ape_dts_timestamp` Int64
)
ENGINE = ReplacingMergeTree(_ape_dts_timestamp)
PRIMARY KEY (pk_1, pk_2, pk_3)
ORDER BY (pk_1, pk_2, pk_3)
SETTINGS index_granularity = 8192