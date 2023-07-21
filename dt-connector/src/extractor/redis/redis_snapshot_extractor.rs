use super::redis_psync_extractor::RedisPsyncExtractor;
use crate::extractor::base_extractor::BaseExtractor;
use crate::Extractor;
use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::error::Error;
use dt_meta::dt_data::DtData;
use redis::Connection;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub struct RedisSnapshotExtractor {
    pub conn: Connection,
    pub buffer: Arc<ConcurrentQueue<DtData>>,
    pub shut_down: Arc<AtomicBool>,
}

#[async_trait]
impl Extractor for RedisSnapshotExtractor {
    async fn extract(&mut self) -> Result<(), Error> {
        let mut psync_extractor = RedisPsyncExtractor {
            conn: &mut self.conn,
            buffer: self.buffer.clone(),
            run_id: "?".to_string(),
            repl_offset: -1,
        };
        psync_extractor.extract().await?;
        BaseExtractor::wait_task_finish(self.buffer.as_ref(), self.shut_down.as_ref()).await
    }
}
