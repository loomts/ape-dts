use super::redis_client::RedisClient;
use super::redis_psync_extractor::RedisPsyncExtractor;
use crate::extractor::base_extractor::BaseExtractor;
use crate::Extractor;
use async_trait::async_trait;
use dt_common::error::Error;
use dt_common::rdb_filter::RdbFilter;

pub struct RedisSnapshotExtractor {
    pub base_extractor: BaseExtractor,
    pub conn: RedisClient,
    pub repl_port: u64,
    pub filter: RdbFilter,
}

#[async_trait]
impl Extractor for RedisSnapshotExtractor {
    async fn extract(&mut self) -> Result<(), Error> {
        let mut psync_extractor = RedisPsyncExtractor {
            base_extractor: &mut self.base_extractor,
            conn: &mut self.conn,
            repl_id: String::new(),
            repl_offset: 0,
            repl_port: self.repl_port,
            now_db_id: 0,
            filter: self.filter.clone(),
        };
        psync_extractor.extract().await?;
        self.base_extractor.wait_task_finish().await
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.conn.close().await
    }
}
