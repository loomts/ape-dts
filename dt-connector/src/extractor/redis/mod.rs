use std::io::{Cursor, Read};

use dt_common::error::Error;

pub mod rdb;
pub mod redis_cdc_extractor;
pub mod redis_client;
pub mod redis_psync_extractor;
pub mod redis_resp_reader;
pub mod redis_resp_types;
pub mod redis_snapshot_extractor;

pub trait RawByteReader {
    fn read_raw(&mut self, size: usize) -> Result<Vec<u8>, Error>;
}

impl RawByteReader for Cursor<&[u8]> {
    fn read_raw(&mut self, size: usize) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0; size];
        self.read_exact(&mut buf)?;
        Ok(buf)
    }
}
