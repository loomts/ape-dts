use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use serde::{Deserialize, Serialize};
use sqlx::{
    postgres::types::{PgMoney, PgTimeTz},
    types::BitVec,
};

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ColValue {
    None,
    Bool(bool),
    Tiny(i8),
    UnsignedTiny(u8),
    Short(i16),
    UnsignedShort(u16),
    Long(i32),
    UnsignedLong(u32),
    LongLong(i64),
    UnsignedLongLong(u64),
    Float(f32),
    Double(f64),
    Decimal(String),
    Time(String),
    Date(String),
    DateTime(String),
    // TODO, add offset
    Timestamp(String),
    Year(u16),
    String(String),
    Blob(Vec<u8>),
    Bit(u64),
    Set(u64),
    Enum(u32),
    Set2(String),
    Enum2(String),
    Json(Vec<u8>),

    PgMoney(PgMoney),
    IpNetwork(sqlx::types::ipnetwork::IpNetwork),
    BitVec(BitVec),
    Interval(Option<sqlx::postgres::types::PgInterval>),
    PgTimeTz(Option<PgTimeTz>),
    NaiveDateTime(Option<chrono::NaiveDateTime>),
    DateTime2(Option<chrono::DateTime<chrono::Utc>>),
    NaiveDate(Option<chrono::NaiveDate>),
    NaiveTime(Option<chrono::NaiveTime>),
    Uuid(Option<sqlx::types::uuid::Uuid>),
}

impl ColValue {
    pub fn hash_code(&self) -> u64 {
        match self {
            ColValue::Tiny(v) => *v as u64,
            ColValue::UnsignedTiny(v) => *v as u64,
            ColValue::Short(v) => *v as u64,
            ColValue::UnsignedShort(v) => *v as u64,
            ColValue::Long(v) => *v as u64,
            ColValue::UnsignedLong(v) => *v as u64,
            ColValue::LongLong(v) => *v as u64,
            ColValue::UnsignedLongLong(v) => *v as u64,
            ColValue::Year(v) => *v as u64,
            ColValue::Enum(v) => *v as u64,
            _ => {
                let mut hasher = DefaultHasher::new();
                self.to_string().hash(&mut hasher);
                hasher.finish()
            }
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            ColValue::Tiny(v) => v.to_string(),
            ColValue::UnsignedTiny(v) => v.to_string(),
            ColValue::Short(v) => v.to_string(),
            ColValue::UnsignedShort(v) => v.to_string(),
            ColValue::Long(v) => v.to_string(),
            ColValue::UnsignedLong(v) => v.to_string(),
            ColValue::LongLong(v) => v.to_string(),
            ColValue::UnsignedLongLong(v) => v.to_string(),
            ColValue::Float(v) => v.to_string(),
            ColValue::Double(v) => v.to_string(),
            ColValue::Decimal(v) => v.to_string(),
            ColValue::Time(v) => v.to_string(),
            ColValue::Date(v) => v.to_string(),
            ColValue::DateTime(v) => v.to_string(),
            ColValue::Timestamp(v) => v.to_string(),
            ColValue::Year(v) => v.to_string(),
            ColValue::String(v) => v.to_string(),
            ColValue::Blob(v) => format!("{:?}", v),
            ColValue::Bit(v) => v.to_string(),
            ColValue::Set(v) => v.to_string(),
            ColValue::Set2(v) => v.to_string(),
            ColValue::Enum(v) => v.to_string(),
            ColValue::Enum2(v) => v.to_string(),
            ColValue::Json(v) => format!("{:?}", v),
            _ => "none".to_string(),
        }
    }
}
