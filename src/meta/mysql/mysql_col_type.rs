use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub enum MysqlColType {
    Unkown,
    Tiny,
    UnsignedTiny,
    Short,
    UnsignedShort,
    Long,
    UnsignedLong,
    LongLong,
    UnsignedLongLong,
    Float,
    Double,
    Decimal,
    Time,
    Date,
    DateTime,
    // timezone diff with utc in seconds
    // refer: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
    Timestamp { timezone_diff_utc_seconds: i64 },
    Year,
    // for char(length), the maximum length is 255,
    // for varchar(length), the maximum length is 65535
    // refer: https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html
    String { length: u64, charset: String },
    Binary { length: u8 },
    VarBinary { length: u16 },
    Blob,
    Bit,
    Set,
    Enum,
    Json,
}
