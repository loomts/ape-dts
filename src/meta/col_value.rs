use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    io::{Cursor, Seek, SeekFrom},
};

use byteorder::{LittleEndian, ReadBytesExt};
use chrono::{TimeZone, Utc};
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use serde::{Deserialize, Serialize};

use crate::error::Error;

use super::{col_meta::ColMeta, col_type::ColType};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ColValue {
    None,
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

    pub fn parse_time(buf: Vec<u8>) -> Result<ColValue, Error> {
        // for: 13:14:15.456, the result buf is [12, 0, 0, 0, 0, 0, 13, 14, 15, 64, 245, 6, 0]
        let mut cursor = Cursor::new(buf);
        let length = cursor.read_u8()? as usize;
        // TODO: why there are 5 zero bytes before hour?
        cursor.seek(SeekFrom::Current(5))?;
        let time = Self::parese_time_fields(&mut cursor, length - 5)?;
        Ok(ColValue::Time(time))
    }

    pub fn parse_date(buf: Vec<u8>) -> Result<ColValue, Error> {
        let mut cursor = Cursor::new(buf);
        let _length = cursor.read_u8()? as usize;
        let date = Self::parese_date_fields(&mut cursor)?;
        Ok(ColValue::Date(date))
    }

    pub fn parse_datetime(buf: Vec<u8>) -> Result<ColValue, Error> {
        let mut cursor = Cursor::new(buf);
        let datetime = Self::parse_date_time_fields(&mut cursor)?;
        Ok(ColValue::DateTime(datetime))
    }

    pub fn parse_timestamp(buf: Vec<u8>) -> Result<ColValue, Error> {
        let mut cursor = Cursor::new(buf);
        let datetime = Self::parse_date_time_fields(&mut cursor)?;
        Ok(ColValue::Timestamp(datetime))
    }

    fn parse_date_time_fields(cursor: &mut Cursor<Vec<u8>>) -> Result<String, Error> {
        let length = cursor.read_u8()? as usize;
        let date = Self::parese_date_fields(cursor)?;
        let time = Self::parese_time_fields(cursor, length - 4)?;
        Ok(format!("{} {}", date, time))
    }

    fn parese_date_fields(cursor: &mut Cursor<Vec<u8>>) -> Result<String, Error> {
        let year = cursor.read_u16::<LittleEndian>()?;
        let month = cursor.read_u8()?;
        let day = cursor.read_u8()?;
        Ok(format!("{}-{:02}-{:02}", year, month, day))
    }

    fn parese_time_fields(cursor: &mut Cursor<Vec<u8>>, length: usize) -> Result<String, Error> {
        let hour = cursor.read_u8()?;
        let minute = cursor.read_u8()?;
        let second = cursor.read_u8()?;
        if length > 3 {
            let micros = cursor.read_uint::<LittleEndian>(length - 3)?;
            Ok(format!(
                "{:02}:{:02}:{:02}.{:06}",
                hour, minute, second, micros
            ))
        } else {
            Ok(format!("{:02}:{:02}:{:02}", hour, minute, second))
        }
    }

    pub fn from_mysql_column_value(col_meta: &ColMeta, value: ColumnValue) -> ColValue {
        match value {
            ColumnValue::Tiny(v) => {
                if col_meta.typee == ColType::UnsignedTiny {
                    return ColValue::UnsignedTiny(v as u8);
                } else {
                    return ColValue::Tiny(v);
                }
            }

            ColumnValue::Short(v) => {
                if col_meta.typee == ColType::UnsignedShort {
                    return ColValue::UnsignedShort(v as u16);
                } else {
                    return ColValue::Short(v);
                }
            }

            ColumnValue::Long(v) => {
                if col_meta.typee == ColType::UnsignedLong {
                    return ColValue::UnsignedLong(v as u32);
                } else {
                    return ColValue::Long(v);
                }
            }

            ColumnValue::LongLong(v) => {
                if col_meta.typee == ColType::UnsignedLongLong {
                    return ColValue::UnsignedLongLong(v as u64);
                } else {
                    return ColValue::LongLong(v);
                }
            }

            ColumnValue::Float(v) => ColValue::Float(v),
            ColumnValue::Double(v) => ColValue::Double(v),
            ColumnValue::Decimal(v) => ColValue::Decimal(v),
            ColumnValue::Time(v) => ColValue::Time(v),
            ColumnValue::Date(v) => ColValue::Date(v),
            ColumnValue::DateTime(v) => ColValue::DateTime(v),
            ColumnValue::Year(v) => ColValue::Year(v),

            ColumnValue::Timestamp(v) => {
                if let ColType::Timestamp {
                    timezone_diff_utc_seconds,
                } = col_meta.typee
                {
                    // the value parsed from binlog is in millis with UTC
                    let dt = Utc.timestamp_nanos(v * 1000 + timezone_diff_utc_seconds * 1000000000);
                    return ColValue::Timestamp(dt.to_string().replace(" UTC", ""));
                } else {
                    let dt = Utc.timestamp_nanos(v * 1000);
                    return ColValue::Timestamp(dt.to_string().replace(" UTC", ""));
                }
            }

            // char, varchar, binary, varbinary
            ColumnValue::String(v) => {
                // when the type is binary(length), the value shoud be right-padded with '\0' to the specified length,
                // otherwise the comparison will fail. https://dev.mysql.com/doc/refman/8.0/en/binary-varbinary.html
                if let ColType::Binary { length } = col_meta.typee {
                    if length as usize > v.len() {
                        let pad_v: Vec<u8> = vec![0; length as usize - v.len()];
                        let final_v = [v, pad_v].concat();
                        return ColValue::Blob(final_v);
                    }
                }
                return ColValue::Blob(v);
            }

            ColumnValue::Blob(v) => ColValue::Blob(v),
            ColumnValue::Bit(v) => ColValue::Bit(v),
            ColumnValue::Set(v) => ColValue::Set(v),
            ColumnValue::Enum(v) => ColValue::Enum(v),
            ColumnValue::Json(v) => ColValue::Json(v),

            _ => ColValue::None,
        }
    }
}
