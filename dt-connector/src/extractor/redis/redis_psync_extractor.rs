use async_trait::async_trait;
use dt_common::log_position;
use dt_common::utils::rdb_filter::RdbFilter;
use dt_common::{error::Error, log_info};
use dt_meta::dt_data::DtData;
use dt_meta::position::Position;
use dt_meta::redis::redis_entry::RedisEntry;
use dt_meta::redis::redis_object::RedisCmd;

use crate::extractor::base_extractor::BaseExtractor;
use crate::extractor::redis::rdb::rdb_parser::RdbParser;
use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;
use crate::extractor::redis::redis_resp_types::Value;

use crate::extractor::redis::StreamReader;
use crate::Extractor;

use super::redis_client::RedisClient;

pub struct RedisPsyncExtractor<'a> {
    pub base_extractor: &'a mut BaseExtractor,
    pub conn: &'a mut RedisClient,
    pub repl_id: String,
    pub repl_offset: u64,
    pub now_db_id: i64,
    pub repl_port: u64,
    pub filter: RdbFilter,
}

#[async_trait]
impl Extractor for RedisPsyncExtractor<'_> {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!(
            "RedisPsyncExtractor starts, repl_id: {}, repl_offset: {}, now_db_id: {}",
            self.repl_id,
            self.repl_offset,
            self.now_db_id
        );
        if self.start_psync().await? {
            // server won't send rdb if it's NOT full sync
            self.receive_rdb().await?;
        }
        Ok(())
    }
}

impl RedisPsyncExtractor<'_> {
    pub async fn start_psync(&mut self) -> Result<bool, Error> {
        // replconf listening-port [port]
        let repl_port = self.repl_port.to_string();
        let repl_cmd = RedisCmd::from_str_args(&["replconf", "listening-port", &repl_port]);
        log_info!("repl command: {}", repl_cmd.to_string());

        self.conn.send(&repl_cmd).await.unwrap();
        if let Value::Okay = self.conn.read().await.unwrap() {
        } else {
            return Err(Error::ExtractorError(
                "replconf listening-port response is not Ok".into(),
            ));
        }

        let full_sync = self.repl_id.is_empty() && self.repl_offset == 0;
        let (repl_id, repl_offset) = if full_sync {
            ("?".to_string(), "-1".to_string())
        } else {
            (self.repl_id.clone(), self.repl_offset.to_string())
        };

        // PSYNC [repl_id] [offset]
        let psync_cmd = RedisCmd::from_str_args(&["PSYNC", &repl_id, &repl_offset]);
        log_info!("PSYNC command: {}", psync_cmd.to_string());
        self.conn.send(&psync_cmd).await.unwrap();
        let value = self.conn.read().await.unwrap();

        if let Value::Status(s) = value {
            log_info!("PSYNC command response status: {:?}", s);
            if full_sync {
                let tokens: Vec<&str> = s.split_whitespace().collect();
                self.repl_id = tokens[1].to_string();
                self.repl_offset = tokens[2].parse::<u64>().unwrap();
            } else if s != "CONTINUE" {
                return Err(Error::ExtractorError(
                    "PSYNC command response is NOT CONTINUE".into(),
                ));
            }
        } else {
            return Err(Error::ExtractorError(
                "PSYNC command response is NOT status".into(),
            ));
        };
        Ok(full_sync)
    }

    async fn receive_rdb(&mut self) -> Result<(), Error> {
        let mut stream_reader: Box<&mut (dyn StreamReader + Send)> = Box::new(self.conn);
        // format: \n\n\n$<length>\r\n<rdb>
        loop {
            let buf = stream_reader.read_bytes(1).unwrap();
            if buf[0] == b'\n' {
                continue;
            }
            if buf[0] != b'$' {
                panic!("invalid rdb format");
            }
            break;
        }

        // length of rdb data
        let mut rdb_length_str = String::new();
        loop {
            let buf = stream_reader.read_bytes(1).unwrap();
            if buf[0] == b'\n' {
                break;
            }
            if buf[0] != b'\r' {
                rdb_length_str.push(buf[0] as char);
            }
        }
        let rdb_length = rdb_length_str.parse::<usize>().unwrap();

        let reader = RdbReader {
            conn: &mut stream_reader,
            rdb_length,
            position: 0,
            copy_raw: false,
            raw_bytes: Vec::new(),
        };

        let mut parser = RdbParser {
            reader,
            repl_stream_db_id: 0,
            now_db_id: self.now_db_id,
            expire_ms: 0,
            idle: 0,
            freq: 0,
            is_end: false,
        };

        let version = parser.load_meta()?;
        log_info!("source redis version: {:?}", version);

        loop {
            if let Some(entry) = parser.load_entry()? {
                self.now_db_id = entry.db_id;
                Self::push_to_buf(self.base_extractor, &mut self.filter, entry, Position::None)
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

        // this log to mark the snapshot rdb was all received
        let position = Position::Redis {
            repl_id: self.repl_id.clone(),
            repl_port: self.repl_port,
            repl_offset: self.repl_offset,
            now_db_id: parser.now_db_id,
            timestamp: String::new(),
        };
        log_position!("current_position | {}", position.to_string());
        Ok(())
    }

    pub async fn push_to_buf(
        base_extractor: &mut BaseExtractor,
        filter: &mut RdbFilter,
        mut entry: RedisEntry,
        position: Position,
    ) -> Result<(), Error> {
        // currently only support db filter
        let db_id = &entry.db_id.to_string();
        if filter.filter_db(db_id) {
            return Ok(());
        }

        entry.data_size = entry.get_data_malloc_size();
        base_extractor
            .push_dt_data(DtData::Redis { entry }, position)
            .await
    }
}
