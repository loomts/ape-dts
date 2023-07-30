use std::io::Cursor;

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use dt_common::error::Error;

use crate::extractor::redis::CursorExt;

use super::rdb_reader::RdbReader;

impl RdbReader<'_> {
    pub fn read_int_set(&mut self) -> Result<Vec<String>, Error> {
        let buf = self.read_string_raw()?;
        let mut reader = Cursor::new(buf.as_slice());

        let encoding_type = reader.read_u32::<LittleEndian>()? as usize;
        let size = reader.read_u32::<LittleEndian>()?;
        let mut elements = Vec::with_capacity(size as usize);

        for _ in 0..size {
            let buf = reader.read_raw(encoding_type)?;
            let int_str = match encoding_type {
                2 => LittleEndian::read_i16(&buf).to_string(),
                4 => LittleEndian::read_i32(&buf).to_string(),
                8 => LittleEndian::read_i64(&buf).to_string(),
                _ => {
                    return Err(Error::Unexpected {
                        error: format!("unknown int encoding type: {:x}", encoding_type)
                            .to_string(),
                    });
                }
            };
            elements.push(int_str);
        }
        Ok(elements)
    }
}
