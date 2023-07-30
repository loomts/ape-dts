use dt_common::error::Error;

use crate::extractor::redis::RawByteReader;

use super::rdb_reader::RdbReader;

impl RdbReader<'_> {
    pub fn read_byte(&mut self) -> Result<u8, Error> {
        let buf = self.read_raw(1).unwrap();
        Ok(buf[0])
    }
}
