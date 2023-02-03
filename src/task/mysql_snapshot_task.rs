use std::sync::atomic::AtomicBool;

use concurrent_queue::ConcurrentQueue;
use futures::future::{join, join_all};
use sqlx::{MySql, Pool};

use crate::{
    config::{env_var::EnvVar, rdb_to_rdb_snapshot_config::RdbToRdbSnapshotConfig},
    error::Error,
    extractor::{filter::Filter, mysql_snapshot_extractor::MysqlSnapshotExtractor},
    meta::db_meta_manager::DbMetaManager,
    sinker::{mysql_sinker::MysqlSinker, router::Router},
};

use super::task_util::TaskUtil;

pub struct MysqlSnapshotTask<'a> {
    pub config: RdbToRdbSnapshotConfig,
    pub env_var: &'a EnvVar,
}

impl MysqlSnapshotTask<'_> {
    pub async fn start(&self) -> Result<(), Error> {
        let filter = Filter::from_config(&self.config.filter)?;
        let router = Router::from_config(&self.config.router)?;

        let src_conn_pool = TaskUtil::create_mysql_conn_pool(
            &self.config.src_url,
            self.config.src_pool_size,
            self.env_var.is_sqlx_log_enabled(),
        )
        .await?;
        let dst_conn_pool = TaskUtil::create_mysql_conn_pool(
            &self.config.dst_url,
            self.config.dst_pool_size,
            self.env_var.is_sqlx_log_enabled(),
        )
        .await?;

        let mut futures = Vec::new();
        for do_tb in filter.do_tbs.iter() {
            let future = self.start_single(&src_conn_pool, &dst_conn_pool, router.clone(), do_tb);
            futures.push(future);
        }

        let results = join_all(futures).await;
        for res in results {
            if res.is_err() {
                return res;
            }
        }
        Ok(())
    }

    async fn start_single<'a>(
        &self,
        src_conn_pool: &Pool<MySql>,
        dst_conn_pool: &Pool<MySql>,
        router: Router,
        do_tb: &str,
    ) -> Result<(), Error> {
        let vec = do_tb.split(".").collect::<Vec<&str>>();
        let db = vec.get(0).unwrap().to_string();
        let tb = vec.get(1).unwrap().to_string();

        let src_db_meta_manager = DbMetaManager::new(&src_conn_pool).init().await?;
        let dst_db_meta_manager = DbMetaManager::new(&dst_conn_pool).init().await?;
        let buffer = ConcurrentQueue::bounded(self.config.buffer_size);
        let shut_down = AtomicBool::new(false);

        let mut extractor = MysqlSnapshotExtractor {
            conn_pool: &src_conn_pool,
            db_meta_manager: src_db_meta_manager,
            buffer: &buffer,
            db,
            tb,
            shut_down: &&shut_down,
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
