use super::redis_psync_extractor::RedisPsyncExtractor;
use crate::Extractor;
use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::error::Error;
use dt_common::syncer::Syncer;
use dt_common::utils::position_util::PositionUtil;
use dt_common::utils::time_util::TimeUtil;
use dt_meta::dt_data::DtData;
use dt_meta::redis::redis_entry::RedisEntry;
use redis::Connection;
use redis::Value;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

pub struct RedisCdcExtractor {
    pub conn: Connection,
    pub buffer: Arc<ConcurrentQueue<DtData>>,
    pub run_id: String,
    pub repl_offset: u64,
    pub heartbeat_interval_secs: u64,
    pub shut_down: Arc<AtomicBool>,
    pub syncer: Arc<Mutex<Syncer>>,
}

#[async_trait]
impl Extractor for RedisCdcExtractor {
    async fn extract(&mut self) -> Result<(), Error> {
        let mut psync_extractor = RedisPsyncExtractor {
            conn: &mut self.conn,
            buffer: self.buffer.clone(),
            run_id: self.run_id.clone(),
            repl_offset: self.repl_offset as i128,
        };
        psync_extractor.extract().await?;
        self.receive_aof().await
    }
}

impl RedisCdcExtractor {
    async fn receive_aof(&mut self) -> Result<(), Error> {
        let mut start_time = Instant::now();
        loop {
            // heartbeat
            if start_time.elapsed().as_secs() > self.heartbeat_interval_secs {
                self.heartbeat()?;
                start_time = Instant::now();
            }

            match self.conn.recv_response().unwrap() {
                Value::Bulk(values) => {
                    let mut entry = RedisEntry::new();
                    let mut i = 0;
                    for v in values {
                        match v {
                            Value::Data(data) => {
                                let arg = String::from_utf8(data).unwrap();
                                if i == 0 && arg.to_lowercase() == "ping" {
                                    break;
                                }
                                entry.argv.push(arg);
                            }
                            _ => {
                                println!("received unexpected aof value: {:?}", v);
                            }
                        }
                        i += 1;
                    }

                    // build entry and push it to buffer
                    if entry.argv.is_empty() {
                        continue;
                    }
                    entry.cmd_name = entry.argv[0].clone();
                    // TODO, get current repl offset
                    entry.position = format!("{}:{}", self.run_id, self.repl_offset);

                    while self.buffer.is_full() {
                        TimeUtil::sleep_millis(1).await;
                    }
                    self.buffer.push(DtData::Redis { entry }).unwrap();
                }
                _ => {}
            }
        }
    }

    fn heartbeat(&mut self) -> Result<(), Error> {
        let position = self.syncer.lock().unwrap().checkpoint_position.clone();
        let repl_offset = if !position.is_empty() {
            let position_info = PositionUtil::parse(&position);
            position_info
                .get("repl_offset")
                .unwrap()
                .parse::<u64>()
                .unwrap()
        } else {
            self.repl_offset as u64
        };

        let cmd = format!("replconf ack {}\r\n", repl_offset);
        println!("heartbeat cmd: {}", cmd);
        let _ = self.conn.send_packed_command(cmd.as_bytes());
        Ok(())
    }
}
