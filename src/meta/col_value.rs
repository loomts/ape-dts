use std::io::{Cursor, Seek, SeekFrom};

use byteorder::{LittleEndian, ReadBytesExt};
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
    Set(String),
    Enum(String),
    Json(Vec<u8>),
}

impl ColValue {
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

    pub fn from_mysql_column_value(meta: &ColMeta, value: ColumnValue) -> ColValue {
        match value {
            ColumnValue::Tiny(v) => {
                if meta.typee == ColType::UnsignedTiny {
                    return ColValue::UnsignedTiny(v as u8);
                } else {
                    return ColValue::Tiny(v);
                }
            }

            ColumnValue::Short(v) => {
                if meta.typee == ColType::UnsignedShort {
                    return ColValue::UnsignedShort(v as u16);
                } else {
                    return ColValue::Short(v);
                }
            }

            ColumnValue::Long(v) => {
                if meta.typee == ColType::UnsignedLong {
                    return ColValue::UnsignedLong(v as u32);
                } else {
                    return ColValue::Long(v);
                }
            }

            ColumnValue::LongLong(v) => {
                if meta.typee == ColType::UnsignedLongLong {
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
            // ColumnValue::Timestamp(v) => ColValue::Timestamp(v),
            ColumnValue::Year(v) => ColValue::Year(v),

            // char, varchar, binary, varbinary
            ColumnValue::String(v) => {
                // when the type is binary(length), the value shoud be right-padded with '\0' to the specified length,
                // otherwise the comparison will fail. https://dev.mysql.com/doc/refman/8.0/en/binary-varbinary.html
                if let ColType::Binary(length) = meta.typee {
                    if length as usize > v.len() {
                        let pad_v: Vec<u8> = vec![0; length as usize - v.len()];
                        let final_v = [v, pad_v].concat();
                        return ColValue::Blob(final_v);
                    }
                }
                return ColValue::Blob(v);
            }

            ColumnValue::Blob(v) => ColValue::Blob(v),
            // ColumnValue::Bit(v) => ColValue::Bit(v),
            // ColumnValue::Set(v) => ColValue::Set(v),
            // ColumnValue::Enum(v) => ColValue::Enum(v),
            ColumnValue::Json(v) => ColValue::Json(v),

            _ => ColValue::None,
        }
    }
}
