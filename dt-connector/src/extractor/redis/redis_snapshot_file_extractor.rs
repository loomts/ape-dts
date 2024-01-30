use std::fs::{self, File};
use std::io::Read;

use super::StreamReader;
use crate::extractor::base_extractor::BaseExtractor;
use crate::extractor::redis::rdb::rdb_parser::RdbParser;
use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;
use crate::extractor::redis::redis_psync_extractor::RedisPsyncExtractor;
use crate::Extractor;
use async_trait::async_trait;
use dt_common::error::Error;
use dt_common::log_info;
use dt_common::utils::rdb_filter::RdbFilter;
use dt_meta::position::Position;

pub struct RedisSnapshotFileExtractor {
    pub file_path: String,
    pub filter: RdbFilter,
    pub base_extractor: BaseExtractor,
}

struct RdbFileReader {
    pub file: File,
}

#[async_trait]
impl Extractor for RedisSnapshotFileExtractor {
    async fn extract(&mut self) -> Result<(), Error> {
        let file = File::open(&self.file_path).expect("rdb file not found");
        let metadata = fs::metadata(&self.file_path).expect("rdb file with wrong meta");
        let mut file_reader = RdbFileReader { file };
        let mut stream_reader: Box<&mut (dyn StreamReader + Send)> = Box::new(&mut file_reader);

        let reader = RdbReader {
            conn: &mut stream_reader,
            rdb_length: metadata.len() as usize,
            position: 0,
            copy_raw: false,
            raw_bytes: Vec::new(),
        };

        let mut parser = RdbParser {
            reader,
            repl_stream_db_id: 0,
            now_db_id: 0,
            expire_ms: 0,
            idle: 0,
            freq: 0,
            is_end: false,
        };

        let version = parser.load_meta()?;
        log_info!("source redis version: {:?}", version);

        loop {
            if let Some(entry) = parser.load_entry()? {
                RedisPsyncExtractor::push_to_buf(
                    &mut self.base_extractor,
                    &mut self.filter,
                    entry,
                    Position::None,
                )
                .await?;
            }

            if parser.is_end {
                log_info!(
                    "end extracting data from rdb, all count: {}",
                    self.base_extractor.monitor.counters.record_count
                );
                break;
            }
        }
        self.base_extractor.wait_task_finish().await
    }
}

impl StreamReader for RdbFileReader {
    fn read_bytes(&mut self, size: usize) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0; size];
        self.file.read_exact(&mut buf).unwrap();
        Ok(buf)
    }
}
