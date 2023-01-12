use std::sync::atomic::AtomicBool;

use concurrent_queue::ConcurrentQueue;
use futures::future::{join, join_all};
use sqlx::{MySql, Pool};

use crate::{
    config::rdb_to_rdb_config::RdbToRdbConfig,
    error::Error,
    extractor::mysql_snapshot_extractor::MysqlSnapshotExtractor,
    meta::db_meta_manager::DbMetaManager,
    sinker::{mysql_sinker::MysqlSinker, router::Router},
};

use super::mysql_task_util::MysqlTaskUtil;

pub struct MysqlSnapshotTask {
    pub config: RdbToRdbConfig,
}

impl MysqlSnapshotTask {
    pub async fn start(&self) -> Result<(), Error> {
        let (filter, router, src_conn_pool, dst_conn_pool) =
            MysqlTaskUtil::init_components(&self.config).await?;

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

        let src_db_meta_manager = DbMetaManager {
            conn_pool: &src_conn_pool,
        };
        let dst_db_meta_manager = DbMetaManager {
            conn_pool: &dst_conn_pool,
        };
        let buffer = ConcurrentQueue::bounded(self.config.buffer_size);
        let shut_down = AtomicBool::new(false);

        let extractor = MysqlSnapshotExtractor {
            conn_pool: &src_conn_pool,
            db_meta_manager: &src_db_meta_manager,
            buffer: &buffer,
            db,
            tb,
            shut_down: &&shut_down,
        };

        let mut sinker = MysqlSinker {
            conn_pool: &dst_conn_pool,
            db_meta_manager: &dst_db_meta_manager,
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
