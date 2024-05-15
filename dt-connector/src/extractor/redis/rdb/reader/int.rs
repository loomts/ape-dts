use crate::extractor::redis::StreamReader;

use super::rdb_reader::RdbReader;
use byteorder::{BigEndian, ByteOrder, LittleEndian};

impl RdbReader<'_> {
    pub fn read_u8(&mut self) -> anyhow::Result<u8> {
        self.read_byte()
    }

    pub fn read_u16(&mut self) -> anyhow::Result<u16> {
        let buf = self.read_bytes(2)?;
        Ok(LittleEndian::read_u16(&buf))
    }

    pub fn read_u24(&mut self) -> anyhow::Result<u32> {
        let buf = self.read_bytes(3)?;
        Ok(LittleEndian::read_u24(&buf))
    }

    pub fn read_u32(&mut self) -> anyhow::Result<u32> {
        let buf = self.read_bytes(4)?;
        Ok(LittleEndian::read_u32(&buf))
    }

    pub fn read_u64(&mut self) -> anyhow::Result<u64> {
        let buf = self.read_bytes(8)?;
        Ok(LittleEndian::read_u64(&buf))
    }

    pub fn read_be_u64(&mut self) -> anyhow::Result<u64> {
        let buf = self.read_bytes(8)?;
        Ok(BigEndian::read_u64(&buf))
    }

    pub fn read_i8(&mut self) -> anyhow::Result<i8> {
        Ok(self.read_byte()? as i8)
    }

    pub fn read_i16(&mut self) -> anyhow::Result<i16> {
        let buf = self.read_bytes(2)?;
        Ok(LittleEndian::read_i16(&buf))
    }

    pub fn read_i24(&mut self) -> anyhow::Result<i32> {
        let buf = self.read_bytes(3)?;
        Ok(LittleEndian::read_i24(&buf))
    }

    pub fn read_i32(&mut self) -> anyhow::Result<i32> {
        let buf = self.read_bytes(4)?;
        Ok(LittleEndian::read_i32(&buf))
    }

    pub fn read_i64(&mut self) -> anyhow::Result<i64> {
        let buf = self.read_bytes(8)?;
        Ok(LittleEndian::read_i64(&buf))
    }

    pub fn read_be_i64(&mut self) -> anyhow::Result<i64> {
        let buf = self.read_bytes(8)?;
        Ok(BigEndian::read_i64(&buf))
    }
}
