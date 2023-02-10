use std::sync::atomic::AtomicBool;

use concurrent_queue::ConcurrentQueue;
use futures::future::join;
use sqlx::{MySql, Pool};

use crate::{
    config::{env_var::EnvVar, rdb_to_rdb_snapshot_config::RdbToRdbSnapshotConfig},
    error::Error,
    extractor::{
        filter::Filter, mysql_snapshot_extractor::MysqlSnapshotExtractor, traits::Extractor,
    },
    meta::db_meta_manager::DbMetaManager,
    sinker::{
        mysql_sinker::MysqlSinker, parallel_sinker::ParallelSinker, router::Router, slicer::Slicer,
        traits::Sinker,
    },
};

use super::task_util::TaskUtil;

pub struct MysqlSnapshotTask {
    pub config: RdbToRdbSnapshotConfig,
    pub env_var: EnvVar,
}

impl MysqlSnapshotTask {
    pub async fn start(&self) -> Result<(), Error> {
        let filter = Filter::from_config(&self.config.filter)?;
        let router = Router::from_config(&self.config.router)?;

        // max_connections: 1 for extracting data from table, 1 for db-meta-manager
        let src_conn_pool = TaskUtil::create_mysql_conn_pool(
            &self.config.src_url,
            2,
            self.env_var.is_sqlx_log_enabled(),
        )
        .await?;

        // max_connections = self.config.parallel_count as u32 + 1 (for db-meta-manager in parallel-sinker)
        let dst_conn_pool = TaskUtil::create_mysql_conn_pool(
            &self.config.dst_url,
            self.config.parallel_count as u32 + 1,
            self.env_var.is_sqlx_log_enabled(),
        )
        .await?;

        for do_tb in filter.do_tbs.iter() {
            self.start_single(
                src_conn_pool.clone(),
                dst_conn_pool.clone(),
                router.clone(),
                do_tb,
            )
            .await?;
        }

        Ok(())
    }

    async fn start_single<'a>(
        &self,
        src_conn_pool: Pool<MySql>,
        dst_conn_pool: Pool<MySql>,
        router: Router,
        do_tb: &str,
    ) -> Result<(), Error> {
        let vec = do_tb.split(".").collect::<Vec<&str>>();
        let db = vec.get(0).unwrap().to_string();
        let tb = vec.get(1).unwrap().to_string();

        let src_db_meta_manager = DbMetaManager::new(src_conn_pool.clone()).init().await?;
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

        let mut extractor = MysqlSnapshotExtractor {
            conn_pool: src_conn_pool.clone(),
            db_meta_manager: src_db_meta_manager,
            buffer: &buffer,
            db,
            tb,
            slice_size: self.config.buffer_size,
            shut_down: &&shut_down,
        };

        let result = join(extractor.extract(), parallel_sinker.sink()).await;
        if result.0.is_err() {
            return result.0;
        }
        Ok(())
    }
}
