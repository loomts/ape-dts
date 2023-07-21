use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::utils::time_util::TimeUtil;
use dt_common::{error::Error, log_info};
use dt_meta::dt_data::DtData;
use redis::Connection;

use std::sync::Arc;

use crate::extractor::redis::rdb::rdb_loader::RdbLoader;
use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;
use crate::Extractor;

pub struct RedisPsyncExtractor<'a> {
    pub conn: &'a mut Connection,
    pub buffer: Arc<ConcurrentQueue<DtData>>,
    pub run_id: String,
    pub repl_offset: i128,
}

#[async_trait]
impl Extractor for RedisPsyncExtractor<'_> {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!(
            "RedisPsyncExtractor starts, run_id: {}, repl_offset: {}",
            self.run_id,
            self.repl_offset,
        );
        self.start_psync().await
    }
}

impl RedisPsyncExtractor<'_> {
    pub async fn start_psync(&mut self) -> Result<(), Error> {
        self.conn
            .send_packed_command(b"replconf listening-port 10007\r\n")
            .unwrap();
        self.conn.recv_response().unwrap();

        // start psync
        let psync_cmd = format!("PSYNC {} {}\r\n", self.run_id, self.repl_offset);
        self.conn.send_packed_command(psync_cmd.as_bytes()).unwrap();

        // parse psync response
        let (run_id, master_offset) =
            if let redis::Value::Status(s) = self.conn.recv_response().unwrap() {
                println!("PSYNC command response status: {:?}", s);
                let tokens: Vec<&str> = s.split_whitespace().collect();
                (tokens[1].to_string(), tokens[2].parse::<u64>().unwrap())
            } else {
                return Err(Error::Unexpected {
                    error: "PSYNC command response is NOT status".to_string(),
                });
            };
        log_info!("run_id: {:?}, master_offset: {:?}", run_id, master_offset);
        self.run_id = run_id;
        self.repl_offset = master_offset as i128;

        // format: \n\n\n$<length>\r\n<rdb>
        loop {
            let buf = self.conn.recv_response_raw(1).unwrap();
            if buf[0] == b'\n' {
                continue;
            }
            if buf[0] != b'$' {
                return Err(Error::Unexpected {
                    error: "invalid rdb format".to_string(),
                });
            }
            break;
        }

        // length of rdb data
        let mut rdb_length_str = String::new();
        loop {
            let buf = self.conn.recv_response_raw(1).unwrap();
            if buf[0] == b'\n' {
                break;
            }
            if buf[0] != b'\r' {
                rdb_length_str.push(buf[0] as char);
            }
        }
        let rdb_length = rdb_length_str.parse::<usize>().unwrap();

        self.receive_rdb(rdb_length).await
    }

    async fn receive_rdb(&mut self, total_length: usize) -> Result<(), Error> {
        let reader = RdbReader {
            conn: &mut self.conn,
            total_length,
            position: 0,
            copy_raw: false,
            raw_bytes: Vec::new(),
        };

        let mut loader = RdbLoader {
            reader,
            repl_stream_db_id: 0,
            now_db_id: 0,
            expire_ms: 0,
            idle: 0,
            freq: 0,
            is_end: false,
        };

        let version = loader.load_meta()?;
        println!("source redis version: {:?}", version);

        loop {
            let entry = loader.load_entry()?;
            if let Some(e) = entry {
                while self.buffer.is_full() {
                    TimeUtil::sleep_millis(1).await;
                }
                self.buffer.push(DtData::Redis { entry: e }).unwrap();
            }

            if loader.is_end {
                break;
            }
        }
        Ok(())
    }
}
