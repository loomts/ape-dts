use crate::extractor::redis::RawByteReader;

use super::rdb_reader::RdbReader;
use byteorder::{BigEndian, ByteOrder, LittleEndian};
use dt_common::error::Error;

impl RdbReader<'_> {
    pub fn read_u8(&mut self) -> Result<u8, Error> {
        self.read_byte()
    }

    pub fn read_u16(&mut self) -> Result<u16, Error> {
        let buf = self.read_raw(2)?;
        Ok(LittleEndian::read_u16(&buf))
    }

    pub fn read_u24(&mut self) -> Result<u32, Error> {
        let buf = self.read_raw(3)?;
        Ok(LittleEndian::read_u24(&buf))
    }

    pub fn read_u32(&mut self) -> Result<u32, Error> {
        let buf = self.read_raw(4)?;
        Ok(LittleEndian::read_u32(&buf))
    }

    pub fn read_u64(&mut self) -> Result<u64, Error> {
        let buf = self.read_raw(8)?;
        Ok(LittleEndian::read_u64(&buf))
    }

    pub fn read_be_u64(&mut self) -> Result<u64, Error> {
        let buf = self.read_raw(8)?;
        Ok(BigEndian::read_u64(&buf))
    }

    pub fn read_i8(&mut self) -> Result<i8, Error> {
        Ok(self.read_byte()? as i8)
    }

    pub fn read_i16(&mut self) -> Result<i16, Error> {
        let buf = self.read_raw(2)?;
        Ok(LittleEndian::read_i16(&buf))
    }

    pub fn read_i24(&mut self) -> Result<i32, Error> {
        let buf = self.read_raw(3)?;
        Ok(LittleEndian::read_i24(&buf))
    }

    pub fn read_i32(&mut self) -> Result<i32, Error> {
        let buf = self.read_raw(4)?;
        Ok(LittleEndian::read_i32(&buf))
    }

    pub fn read_i64(&mut self) -> Result<i64, Error> {
        let buf = self.read_raw(8)?;
        Ok(LittleEndian::read_i64(&buf))
    }

    pub fn read_be_i64(&mut self) -> Result<i64, Error> {
        let buf = self.read_raw(8)?;
        Ok(BigEndian::read_i64(&buf))
    }
}
