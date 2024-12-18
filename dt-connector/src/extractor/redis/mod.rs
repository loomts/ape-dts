use std::io::{Cursor, Read};

pub mod rdb;
pub mod redis_client;
pub mod redis_psync_extractor;
pub mod redis_reshard_extractor;
pub mod redis_resp_reader;
pub mod redis_resp_types;
pub mod redis_scan_extractor;
pub mod redis_snapshot_file_extractor;

pub trait StreamReader {
    fn read_bytes(&mut self, size: usize) -> anyhow::Result<Vec<u8>>;
}

impl StreamReader for Cursor<&[u8]> {
    fn read_bytes(&mut self, size: usize) -> anyhow::Result<Vec<u8>> {
        let mut buf = vec![0; size];
        self.read_exact(&mut buf)?;
        Ok(buf)
    }
}
