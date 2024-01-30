use byteorder::{ByteOrder, LittleEndian};
use dt_common::error::Error;

use crate::extractor::redis::StreamReader;

use super::rdb_reader::RdbReader;

impl RdbReader<'_> {
    pub fn read_float(&mut self) -> Result<f64, Error> {
        let n = self.read_u8()?;
        let v = match n {
            253 => f64::NAN,
            254 => f64::INFINITY,
            255 => f64::NEG_INFINITY,
            _ => {
                let buf = self.read_bytes(n as usize)?;
                let s = String::from_utf8(buf).unwrap();
                let v: f64 = s.parse().unwrap();
                v
            }
        };
        Ok(v)
    }

    pub fn read_double(&mut self) -> Result<f64, Error> {
        let buf = self.read_bytes(8)?;
        Ok(LittleEndian::read_f64(&buf))
    }
}
