use super::redis_client::RedisClient;
use super::redis_psync_extractor::RedisPsyncExtractor;
use super::redis_resp_types::Value;
use crate::Extractor;
use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::error::Error;
use dt_common::log_error;
use dt_common::log_info;
use dt_common::syncer::Syncer;
use dt_common::utils::position_util::PositionUtil;
use dt_common::utils::time_util::TimeUtil;
use dt_meta::dt_data::DtData;
use dt_meta::redis::redis_entry::RedisEntry;
use dt_meta::redis::redis_object::RedisCmd;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

pub struct RedisCdcExtractor {
    pub conn: RedisClient,
    pub buffer: Arc<ConcurrentQueue<DtData>>,
    pub run_id: String,
    pub repl_offset: u64,
    pub repl_port: u64,
    pub now_db_id: i64,
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
            repl_offset: self.repl_offset,
            repl_port: self.repl_port,
            now_db_id: self.now_db_id,
        };

        // receive rdb data if needed
        psync_extractor.extract().await?;
        self.run_id = psync_extractor.run_id;
        self.repl_offset = psync_extractor.repl_offset;

        self.receive_aof().await
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.conn.close().await
    }
}

impl RedisCdcExtractor {
    async fn receive_aof(&mut self) -> Result<(), Error> {
        let mut start_time = Instant::now();
        loop {
            // heartbeat
            if start_time.elapsed().as_secs() > self.heartbeat_interval_secs {
                self.heartbeat().await?;
                start_time = Instant::now();
            }

            let (value, n) = self.conn.read_with_len().await.unwrap();
            if Value::Nil == value {
                continue;
            }

            self.repl_offset += n as u64;
            let cmd = self.handle_redis_value(value).await.unwrap();

            if !cmd.args.is_empty() {
                if cmd.get_name().eq_ignore_ascii_case("select") {
                    self.now_db_id = String::from_utf8(cmd.args[1].clone())
                        .unwrap()
                        .parse::<i64>()
                        .unwrap();
                    continue;
                }

                // build entry and push it to buffer
                let mut entry = RedisEntry::new();
                entry.cmd = cmd;
                entry.db_id = self.now_db_id;
                entry.position = format!(
                    "run_id:{},repl_offset:{},now_db_id:{}",
                    self.run_id, self.repl_offset, self.now_db_id
                );
                self.push_to_buf(entry).await;
            }
        }
    }

    async fn push_to_buf(&mut self, entry: RedisEntry) {
        while self.buffer.is_full() {
            TimeUtil::sleep_millis(1).await;
        }
        self.buffer.push(DtData::Redis { entry }).unwrap();
    }

    async fn handle_redis_value(&mut self, value: Value) -> Result<RedisCmd, Error> {
        let mut cmd = RedisCmd::new();
        match value {
            Value::Bulk(values) => {
                for v in values {
                    match v {
                        Value::Data(data) => cmd.add_arg(data),
                        _ => {
                            log_error!("received unexpected value in aof bulk: {:?}", v);
                            break;
                        }
                    }
                }
            }
            v => {
                return Err(Error::Unexpected {
                    error: format!("received unexpected aof value: {:?}", v),
                });
            }
        }
        Ok(cmd)
    }

    async fn heartbeat(&mut self) -> Result<(), Error> {
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

        let repl_offset = &repl_offset.to_string();
        let args = vec!["replconf", "ack", repl_offset];
        let cmd = RedisCmd::from_str_args(&args);
        log_info!("heartbeat cmd: {:?}", cmd);
        let _ = self.conn.send(&cmd).await;
        Ok(())
    }
}
