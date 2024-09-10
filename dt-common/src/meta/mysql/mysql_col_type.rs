use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Display)]
pub enum MysqlColType {
    Unkown,
    Tiny,
    UnsignedTiny,
    Short,
    UnsignedShort,
    Medium,
    UnsignedMedium,
    Long,
    UnsignedLong,
    LongLong,
    UnsignedLongLong,
    Float,
    Double,
    Decimal { precision: u32, scale: u32 },
    Time,
    Date,
    DateTime,
    // timezone diff with utc in seconds
    // refer: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
    Timestamp { timezone_offset: i64 },
    Year,
    // for char(length), the maximum length is 255,
    // for varchar(length), the maximum length is 65535
    // refer: https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html
    String { length: u64, charset: String },
    Binary { length: u8 },
    VarBinary { length: u16 },
    Blob,
    Bit,
    Set { items: HashMap<u64, String> },
    Enum { items: HashMap<u32, String> },
    Json,
}
