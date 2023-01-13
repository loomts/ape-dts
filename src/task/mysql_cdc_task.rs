use futures::future::join;
use std::sync::atomic::AtomicBool;

use concurrent_queue::ConcurrentQueue;

use crate::{
    config::rdb_to_rdb_config::RdbToRdbConfig, error::Error,
    extractor::mysql_cdc_extractor::MysqlCdcExtractor, meta::db_meta_manager::DbMetaManager,
    sinker::mysql_sinker::MysqlSinker,
};

use super::mysql_task_util::MysqlTaskUtil;

pub struct MysqlCdcTask {
    pub config: RdbToRdbConfig,
}

impl MysqlCdcTask {
    pub async fn start(&self) -> Result<(), Error> {
        let (filter, router, src_conn_pool, dst_conn_pool) =
            MysqlTaskUtil::init_components(&self.config).await?;
        let src_db_meta_manager = DbMetaManager::new(&src_conn_pool).init().await?;
        let dst_db_meta_manager = DbMetaManager::new(&dst_conn_pool).init().await?;
        let buffer = ConcurrentQueue::bounded(self.config.buffer_size);
        let shut_down = AtomicBool::new(false);

        let mut extractor = MysqlCdcExtractor {
            db_meta_manager: src_db_meta_manager,
            buffer: &buffer,
            filter,
            url: self.config.src_url.clone(),
            binlog_filename: "".to_string(),
            binlog_position: 0,
            server_id: 200,
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
