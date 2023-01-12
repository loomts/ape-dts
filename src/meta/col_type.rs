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
    String,
    Blob,
    Bit,
    Set,
    Enum,
    Json,
}
