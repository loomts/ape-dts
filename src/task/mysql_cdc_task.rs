use futures::future::join;
use sqlx::mysql::MySqlPoolOptions;
use std::sync::atomic::AtomicBool;

use concurrent_queue::ConcurrentQueue;

use crate::{
    config::mysql_to_rdb_cdc_config::MysqlToRdbCdcConfig,
    error::Error,
    extractor::{filter::Filter, mysql_cdc_extractor::MysqlCdcExtractor},
    meta::db_meta_manager::DbMetaManager,
    sinker::{mysql_sinker::MysqlSinker, router::Router},
};

pub struct MysqlCdcTask {
    pub config: MysqlToRdbCdcConfig,
}

impl MysqlCdcTask {
    pub async fn start(&self) -> Result<(), Error> {
        let filter = Filter::from_config(&self.config.filter)?;
        let router = Router::from_config(&self.config.router)?;
        let src_conn_pool = MySqlPoolOptions::new()
            .max_connections(1)
            .connect(&self.config.src_url)
            .await?;
        let dst_conn_pool = MySqlPoolOptions::new()
            .max_connections(1)
            .connect(&self.config.dst_url)
            .await?;

        let src_db_meta_manager = DbMetaManager::new(&src_conn_pool).init().await?;
        let dst_db_meta_manager = DbMetaManager::new(&dst_conn_pool).init().await?;
        let buffer = ConcurrentQueue::bounded(self.config.buffer_size);
        let shut_down = AtomicBool::new(false);

        let mut extractor = MysqlCdcExtractor {
            db_meta_manager: src_db_meta_manager,
            buffer: &buffer,
            filter,
            url: self.config.src_url.clone(),
            binlog_filename: self.config.binlog_filename.clone(),
            binlog_position: self.config.binlog_position,
            server_id: self.config.server_id,
            shut_down: &shut_down,
        };

        let mut sinker = MysqlSinker {
            conn_pool: &dst_conn_pool,
            db_meta_manager: dst_db_meta_manager,
            buffer: &buffer,
            router,
            shut_down: &shut_down,
        };

        let extract_future = extractor.extract();
        let apply_future = sinker.sink();

        let (res1, res2) = join(extract_future, apply_future).await;
        if res1.is_err() {
            return res1;
        } else {
            return res2;
        }
    }
}
