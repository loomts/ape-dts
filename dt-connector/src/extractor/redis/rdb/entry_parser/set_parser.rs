use std::io::Cursor;

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use dt_common::error::Error;
use dt_meta::redis::redis_object::{RedisString, SetObject};

use crate::extractor::redis::{rdb::reader::rdb_reader::RdbReader, StreamReader};

pub struct SetParser {}

impl SetParser {
    pub fn load_from_buffer(
        reader: &mut RdbReader,
        key: RedisString,
        type_byte: u8,
    ) -> Result<SetObject, Error> {
        let mut obj = SetObject::new();
        obj.key = key;

        match type_byte {
            super::RDB_TYPE_SET => Self::read_str_set(&mut obj, reader)?,
            super::RDB_TYPE_SET_INTSET => Self::read_int_set(&mut obj, reader)?,
            super::RDB_TYPE_SET_LISTPACK => obj.elements = reader.read_list_pack()?,
            _ => {
                return Err(Error::RedisRdbError(format!(
                    "unknown set type. type_byte=[{}]",
                    type_byte
                )))
            }
        }
        Ok(obj)
    }

    pub fn read_str_set(obj: &mut SetObject, reader: &mut RdbReader) -> Result<(), Error> {
        let size = reader.read_length()? as usize;
        for _i in 0..size {
            obj.elements.push(reader.read_string()?);
        }
        Ok(())
    }

    pub fn read_int_set(obj: &mut SetObject, reader: &mut RdbReader) -> Result<(), Error> {
        let buf = reader.read_string()?;
        let mut reader = Cursor::new(buf.as_bytes());

        let encoding_type = reader.read_u32::<LittleEndian>()? as usize;
        let size = reader.read_u32::<LittleEndian>()?;
        for _ in 0..size {
            let buf = reader.read_bytes(encoding_type)?;
            let int_str = match encoding_type {
                2 => LittleEndian::read_i16(&buf).to_string(),
                4 => LittleEndian::read_i32(&buf).to_string(),
                8 => LittleEndian::read_i64(&buf).to_string(),
                _ => {
                    return Err(Error::RedisRdbError(format!(
                        "unknown int encoding type: {:x}",
                        encoding_type
                    )));
                }
            };
            obj.elements.push(RedisString::from(int_str));
        }
        Ok(())
    }
}
