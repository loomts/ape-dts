use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
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
}

impl ColValue {
    pub fn hash_code(&self) -> u64 {
        match self {
            ColValue::None => 0,
            _ => {
                let mut hasher = DefaultHasher::new();
                self.to_string().hash(&mut hasher);
                hasher.finish()
            }
        }
    }

    pub fn to_string(&self) -> Option<String> {
        match self {
            ColValue::Tiny(v) => Some(v.to_string()),
            ColValue::UnsignedTiny(v) => Some(v.to_string()),
            ColValue::Short(v) => Some(v.to_string()),
            ColValue::UnsignedShort(v) => Some(v.to_string()),
            ColValue::Long(v) => Some(v.to_string()),
            ColValue::UnsignedLong(v) => Some(v.to_string()),
            ColValue::LongLong(v) => Some(v.to_string()),
            ColValue::UnsignedLongLong(v) => Some(v.to_string()),
            ColValue::Float(v) => Some(v.to_string()),
            ColValue::Double(v) => Some(v.to_string()),
            ColValue::Decimal(v) => Some(v.to_string()),
            ColValue::Time(v) => Some(v.to_string()),
            ColValue::Date(v) => Some(v.to_string()),
            ColValue::DateTime(v) => Some(v.to_string()),
            ColValue::Timestamp(v) => Some(v.to_string()),
            ColValue::Year(v) => Some(v.to_string()),
            ColValue::String(v) => Some(v.to_string()),
            ColValue::Blob(v) => Some(format!("{:?}", v)),
            ColValue::Bit(v) => Some(v.to_string()),
            ColValue::Set(v) => Some(v.to_string()),
            ColValue::Set2(v) => Some(v.to_string()),
            ColValue::Enum(v) => Some(v.to_string()),
            ColValue::Enum2(v) => Some(v.to_string()),
            ColValue::Json(v) => Some(format!("{:?}", v)),
            _ => Option::None,
        }
    }
}
