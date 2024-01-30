use dt_common::error::Error;

use crate::extractor::redis::StreamReader;

use super::rdb_reader::RdbReader;

impl RdbReader<'_> {
    pub fn read_byte(&mut self) -> Result<u8, Error> {
        let buf = self.read_bytes(1).unwrap();
        Ok(buf[0])
    }
}
