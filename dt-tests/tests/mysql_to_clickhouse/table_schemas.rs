use clickhouse::Row;
use serde::{Deserialize, Serialize};

#[derive(Row, Deserialize, Serialize)]
pub(super) struct MysqlBasicTable {
    pk: i8,
    tinyint_col: Option<i8>,
    tinyint_col_unsigned: Option<u8>,
    smallint_col: Option<i16>,
    smallint_col_unsigned: Option<u16>,
    mediumint_col: Option<i32>,
    mediumint_col_unsigned: Option<u32>,
    int_col: Option<i32>,
    int_col_unsigned: Option<u32>,
    bigint_col: Option<i64>,
    bigint_col_unsigned: Option<u64>,
    // decimal_col: Option<String>,
    // float_col: Option<f32>,
    // double_col: Option<f64>,
    bit_col: Option<u64>,
    // #[serde(with = "clickhouse::serde::time::datetime")]
    // datetime_col: OffsetDateTime,
    time_col: Option<String>,
    // #[serde(with = "clickhouse::serde::time::date")]
    // date_col: Date,
    year_col: Option<i32>,
    // #[serde(with = "clickhouse::serde::time::datetime")]
    // timestamp_col: OffsetDateTime,
    char_col: Option<String>,
    varchar_col: Option<String>,
    binary_col: Option<String>,
    varbinary_col: Option<String>,
    tinytext_col: Option<String>,
    text_col: Option<String>,
    mediumtext_col: Option<String>,
    longtext_col: Option<String>,
    tinyblob_col: Option<String>,
    blob_col: Option<String>,
    mediumblob_col: Option<String>,
    longblob_col: Option<String>,
    enum_col: Option<String>,
    set_col: Option<String>,
    json_col: Option<String>,
    // _ape_dts_is_deleted: i8,
    // _ape_dts_timestamp: i64,
}
