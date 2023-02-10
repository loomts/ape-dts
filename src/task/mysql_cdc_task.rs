use concurrent_queue::ConcurrentQueue;
use futures::future::join;

use std::sync::atomic::AtomicBool;

use crate::{
    config::{env_var::EnvVar, mysql_to_rdb_cdc_config::MysqlToRdbCdcConfig},
    error::Error,
    extractor::{filter::Filter, mysql_cdc_extractor::MysqlCdcExtractor, traits::Extractor},
    meta::db_meta_manager::DbMetaManager,
    sinker::{
        mysql_sinker::MysqlSinker, parallel_sinker::ParallelSinker, router::Router, slicer::Slicer,
        traits::Sinker,
    },
};

use super::task_util::TaskUtil;

pub struct MysqlCdcTask {
    pub config: MysqlToRdbCdcConfig,
    pub env_var: EnvVar,
}

impl MysqlCdcTask {
    pub async fn start(&self) -> Result<(), Error> {
        let filter = Filter::from_config(&self.config.filter)?;
        let router = Router::from_config(&self.config.router)?;

        let src_conn_pool = TaskUtil::create_mysql_conn_pool(
            &self.config.src_url,
            1,
            self.env_var.is_sqlx_log_enabled(),
        )
        .await?;

        let dst_conn_pool = TaskUtil::create_mysql_conn_pool(
            &self.config.dst_url,
            self.config.parallel_count as u32 + 1,
            self.env_var.is_sqlx_log_enabled(),
        )
        .await?;

        let src_db_meta_manager = DbMetaManager::new(src_conn_pool).init().await?;
        let dst_db_meta_manager = DbMetaManager::new(dst_conn_pool.clone()).init().await?;
        let buffer = ConcurrentQueue::bounded(self.config.buffer_size);
        let shut_down = AtomicBool::new(false);

        let mut sub_sinkers: Vec<Box<dyn Sinker>> = Vec::new();
        for _ in 0..self.config.parallel_count {
            let sinker = MysqlSinker {
                conn_pool: dst_conn_pool.clone(),
                db_meta_manager: dst_db_meta_manager.clone(),
                buffer: ConcurrentQueue::unbounded(),
                router: router.clone(),
            };
            sub_sinkers.push(Box::new(sinker));
        }

        let slicer = Slicer {
            db_meta_manager: dst_db_meta_manager.clone(),
        };

        let mut parallel_sinker = ParallelSinker {
            buffer: &buffer,
            slicer,
            sub_sinkers,
            shut_down: &shut_down,
        };

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

        let result = join(extractor.extract(), parallel_sinker.sink()).await;
        if result.0.is_err() {
            return result.0;
        }
        return result.1;
    }
}
