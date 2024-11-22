use serde::{Deserialize, Serialize};
use strum::Display;

// refer to: https://duckdb.org/docs/sql/data_types/overview.html

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Display)]
pub enum DuckdbColType {
    Unknown,
    TinyInt,
    UTinyInt,
    SmallInt,
    USmallInt,
    Integer,
    UInteger,
    BigInt,
    UBigInt,
    Float,
    Double,
    Timestamp,
    Date,
    Interval,
    DateTime,
    Decimal { precision: u32, scale: u32 },
    Varchar,
    Blob,
    Json,
    Enum,
}
