use std::io::{Cursor, Seek, SeekFrom};

use byteorder::{LittleEndian, ReadBytesExt};
use chrono::{TimeZone, Utc};
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use sqlx::{mysql::MySqlRow, Row};

use crate::{
    error::Error,
    meta::{
        col_value::ColValue,
        mysql::{mysql_col_meta::MysqlColMeta, mysql_col_type::MysqlColType},
    },
};

pub struct MysqlColValueConvertor {}

impl MysqlColValueConvertor {
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

    pub fn from_binlog(col_meta: &MysqlColMeta, value: ColumnValue) -> ColValue {
        match value {
            ColumnValue::Tiny(v) => {
                if col_meta.typee == MysqlColType::UnsignedTiny {
                    return ColValue::UnsignedTiny(v as u8);
                } else {
                    return ColValue::Tiny(v);
                }
            }

            ColumnValue::Short(v) => {
                if col_meta.typee == MysqlColType::UnsignedShort {
                    return ColValue::UnsignedShort(v as u16);
                } else {
                    return ColValue::Short(v);
                }
            }

            ColumnValue::Long(v) => {
                if col_meta.typee == MysqlColType::UnsignedLong {
                    return ColValue::UnsignedLong(v as u32);
                } else {
                    return ColValue::Long(v);
                }
            }

            ColumnValue::LongLong(v) => {
                if col_meta.typee == MysqlColType::UnsignedLongLong {
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
                if let MysqlColType::Timestamp {
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
                if let MysqlColType::Binary { length } = col_meta.typee {
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

    pub fn from_str(col_meta: &MysqlColMeta, value_str: &str) -> Result<ColValue, Error> {
        let value_str = value_str.to_string();
        if ColValue::None.to_string() == value_str {
            return Ok(ColValue::None);
        }

        let col_value = match col_meta.typee {
            MysqlColType::Tiny => match value_str.parse::<i8>() {
                Ok(value) => ColValue::Tiny(value),
                Err(_) => ColValue::None,
            },
            MysqlColType::UnsignedTiny => match value_str.parse::<u8>() {
                Ok(value) => ColValue::UnsignedTiny(value),
                Err(_) => ColValue::None,
            },
            MysqlColType::Short => match value_str.parse::<i16>() {
                Ok(value) => ColValue::Short(value),
                Err(_) => ColValue::None,
            },
            MysqlColType::UnsignedShort => match value_str.parse::<u16>() {
                Ok(value) => ColValue::UnsignedShort(value),
                Err(_) => ColValue::None,
            },
            MysqlColType::Long => match value_str.parse::<i32>() {
                Ok(value) => ColValue::Long(value),
                Err(_) => ColValue::None,
            },
            MysqlColType::UnsignedLong => match value_str.parse::<u32>() {
                Ok(value) => ColValue::UnsignedLong(value),
                Err(_) => ColValue::None,
            },
            MysqlColType::LongLong => match value_str.parse::<i64>() {
                Ok(value) => ColValue::LongLong(value),
                Err(_) => ColValue::None,
            },
            MysqlColType::UnsignedLongLong => match value_str.parse::<u64>() {
                Ok(value) => ColValue::UnsignedLongLong(value),
                Err(_) => ColValue::None,
            },
            MysqlColType::Float => match value_str.parse::<f32>() {
                Ok(value) => ColValue::Float(value),
                Err(_) => ColValue::None,
            },
            MysqlColType::Double => match value_str.parse::<f64>() {
                Ok(value) => ColValue::Double(value),
                Err(_) => ColValue::None,
            },

            MysqlColType::Decimal => ColValue::Decimal(value_str),
            MysqlColType::Time => ColValue::Time(value_str),
            MysqlColType::Date => ColValue::Date(value_str),
            MysqlColType::DateTime => ColValue::DateTime(value_str),

            MysqlColType::Timestamp {
                timezone_diff_utc_seconds: _,
            } => ColValue::Timestamp(value_str),

            MysqlColType::Year => match value_str.parse::<u16>() {
                Ok(value) => ColValue::Year(value),
                Err(_) => ColValue::None,
            },

            MysqlColType::String {
                length: _,
                charset: _,
            } => ColValue::String(value_str),

            MysqlColType::Bit => match value_str.parse::<u64>() {
                Ok(value) => ColValue::Bit(value),
                Err(_) => ColValue::None,
            },

            MysqlColType::Set => ColValue::String(value_str),
            MysqlColType::Enum => ColValue::String(value_str),

            _ => {
                return Err(Error::Unexpected {
                    error: format!(
                        "unsupported column type, column: {}, column type: {:?}",
                        col_meta.name, col_meta.typee
                    ),
                })
            }
        };

        Ok(col_value)
    }

    pub fn from_query(row: &MySqlRow, col_meta: &MysqlColMeta) -> Result<ColValue, Error> {
        let col_name: &str = col_meta.name.as_ref();
        let value: Option<Vec<u8>> = row.get_unchecked(col_name);
        if let None = value {
            return Ok(ColValue::None);
        }

        match col_meta.typee {
            MysqlColType::Tiny => {
                let value: i8 = row.try_get(col_name)?;
                return Ok(ColValue::Tiny(value));
            }
            MysqlColType::UnsignedTiny => {
                let value: u8 = row.try_get(col_name)?;
                return Ok(ColValue::UnsignedTiny(value));
            }
            MysqlColType::Short => {
                let value: i16 = row.try_get(col_name)?;
                return Ok(ColValue::Short(value));
            }
            MysqlColType::UnsignedShort => {
                let value: u16 = row.try_get(col_name)?;
                return Ok(ColValue::UnsignedShort(value));
            }
            MysqlColType::Long => {
                let value: i32 = row.try_get(col_name)?;
                return Ok(ColValue::Long(value));
            }
            MysqlColType::UnsignedLong => {
                let value: u32 = row.try_get(col_name)?;
                return Ok(ColValue::UnsignedLong(value));
            }
            MysqlColType::LongLong => {
                let value: i64 = row.try_get(col_name)?;
                return Ok(ColValue::LongLong(value));
            }
            MysqlColType::UnsignedLongLong => {
                let value: u64 = row.try_get(col_name)?;
                return Ok(ColValue::UnsignedLongLong(value));
            }
            MysqlColType::Float => {
                let value: f32 = row.try_get(col_name)?;
                return Ok(ColValue::Float(value));
            }
            MysqlColType::Double => {
                let value: f64 = row.try_get(col_name)?;
                return Ok(ColValue::Double(value));
            }
            MysqlColType::Decimal => {
                let value: String = row.get_unchecked(col_name);
                return Ok(ColValue::Decimal(value));
            }
            MysqlColType::Time => {
                let value: Vec<u8> = row.get_unchecked(col_name);
                return MysqlColValueConvertor::parse_time(value);
            }
            MysqlColType::Date => {
                let value: Vec<u8> = row.get_unchecked(col_name);
                return MysqlColValueConvertor::parse_date(value);
            }
            MysqlColType::DateTime => {
                let value: Vec<u8> = row.get_unchecked(col_name);
                return MysqlColValueConvertor::parse_datetime(value);
            }
            MysqlColType::Timestamp {
                timezone_diff_utc_seconds: _,
            } => {
                let value: Vec<u8> = row.get_unchecked(col_name);
                return MysqlColValueConvertor::parse_timestamp(value);
            }
            MysqlColType::Year => {
                let value: u16 = row.try_get(col_name)?;
                return Ok(ColValue::Year(value));
            }
            MysqlColType::String {
                length: _,
                charset: _,
            } => {
                let value: Vec<u8> = row.try_get(col_name)?;
                return Ok(ColValue::Blob(value));
            }
            MysqlColType::Binary { length: _ } => {
                let value: Vec<u8> = row.try_get(col_name)?;
                return Ok(ColValue::Blob(value));
            }
            MysqlColType::VarBinary { length: _ } => {
                let value: Vec<u8> = row.try_get(col_name)?;
                return Ok(ColValue::Blob(value));
            }
            MysqlColType::Blob => {
                let value: Vec<u8> = row.try_get(col_name)?;
                return Ok(ColValue::Blob(value));
            }
            MysqlColType::Bit => {
                let value: u64 = row.try_get(col_name)?;
                return Ok(ColValue::Bit(value as u64));
            }
            MysqlColType::Set => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::Set2(value));
            }
            MysqlColType::Enum => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::Enum2(value));
            }
            MysqlColType::Json => {
                let value: Vec<u8> = row.get_unchecked(col_name);
                return Ok(ColValue::Json(value));
            }
            _ => {}
        }
        Ok(ColValue::None)
    }
}
