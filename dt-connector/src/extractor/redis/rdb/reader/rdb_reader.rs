use dt_common::error::Error;
use redis::Connection;

use crate::extractor::redis::RawByteReader;

pub struct RdbReader<'a> {
    pub conn: &'a mut Connection,
    pub total_length: usize,
    pub position: usize,
    pub copy_raw: bool,
    pub raw_bytes: Vec<u8>,
}

impl RdbReader<'_> {
    pub fn drain_raw_bytes(&mut self) -> Vec<u8> {
        self.raw_bytes.drain(..).collect()
    }
}

impl RawByteReader for RdbReader<'_> {
    fn read_raw(&mut self, length: usize) -> Result<Vec<u8>, Error> {
        let buf = self.conn.recv_response_raw(length).unwrap();
        self.position += length;
        if self.copy_raw {
            self.raw_bytes.extend_from_slice(&buf);
        }
        Ok(buf)
    }
}
