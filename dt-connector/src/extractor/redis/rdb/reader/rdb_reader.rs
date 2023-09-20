use crate::extractor::redis::{redis_client::RedisClient, RawByteReader};
use dt_common::error::Error;
use futures::executor::block_on;

pub struct RdbReader<'a> {
    pub conn: &'a mut RedisClient,
    pub rdb_length: usize,
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
        let buf = block_on(self.conn.read_raw(length)).unwrap();
        self.position += length;
        if self.copy_raw {
            self.raw_bytes.extend_from_slice(&buf);
        }
        Ok(buf)
    }
}
