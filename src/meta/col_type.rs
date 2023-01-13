use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub enum ColType {
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
    Timestamp,
    Year,
    // String(length, charset)
    // for char(length), the maximum length is 255,
    // for varchar(length), the maximum length is 65535
    // refer: https://dev.mysql.com/doc/refman/5.7/en/storage-requirements.html
    String(u64, String),
    // Binary(length)
    Binary(u8),
    // VarBinary(length)
    VarBinary(u16),
    Blob,
    Bit,
    Set,
    Enum,
    Json,
}
