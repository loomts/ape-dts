use async_trait::async_trait;
use dt_common::{error::Error, rdb_filter::RdbFilter, utils::redis_util::RedisUtil};

use crate::fetcher::traits::Fetcher;

pub struct RedisFetcher {
    pub url: String,
    pub conn: Option<redis::Connection>,
    pub is_source: bool,
    pub filter: RdbFilter,
}

#[async_trait]
impl Fetcher for RedisFetcher {
    async fn build_connection(&mut self) -> Result<(), Error> {
        self.conn = Some(RedisUtil::create_redis_conn(&self.url).await?);
        Ok(())
    }

    async fn fetch_version(&mut self) -> Result<String, Error> {
        let conn = self.conn.as_mut().unwrap();
        let version = RedisUtil::get_redis_version(conn)?;
        Ok(version.to_string())
    }
}

impl RedisFetcher {}
