use std::sync::Arc;

use crate::{
    config::sinker_config::SinkerConfig,
    error::Error,
    meta::{mysql::mysql_meta_manager::MysqlMetaManager, pg::pg_meta_manager::PgMetaManager},
    pipeline::{rdb_merger::RdbMerger, rdb_partitioner::RdbPartitioner},
    sinker::{mysql_sinker::MysqlSinker, pg_sinker::PgSinker, rdb_router::RdbRouter},
    traits::Sinker,
};

use super::task_util::TaskUtil;

pub struct SinkerUtil {}

impl SinkerUtil {
    pub async fn create_rdb_merger(
        log_level: &str,
        sinker_config: &SinkerConfig,
    ) -> Result<RdbMerger, Error> {
        let merger = match &sinker_config {
            SinkerConfig::Mysql { url, .. } => {
                let meta_manager = TaskUtil::create_mysql_meta_manager(&url, log_level).await?;
                RdbMerger::new_for_mysql(meta_manager)
            }

            SinkerConfig::Pg { url, .. } => {
                let meta_manager = TaskUtil::create_pg_meta_manager(&url, log_level).await?;
                RdbMerger::new_for_pg(meta_manager)
            }
        };
        Ok(merger)
    }

    pub async fn create_rdb_partitioner(
        log_level: &str,
        sinker_config: &SinkerConfig,
    ) -> Result<RdbPartitioner, Error> {
        let merger = match &sinker_config {
            SinkerConfig::Mysql { url, .. } => {
                let meta_manager = TaskUtil::create_mysql_meta_manager(&url, log_level).await?;
                RdbPartitioner::new_for_mysql(meta_manager)
            }

            SinkerConfig::Pg { url, .. } => {
                let meta_manager = TaskUtil::create_pg_meta_manager(&url, log_level).await?;
                RdbPartitioner::new_for_pg(meta_manager)
            }
        };
        Ok(merger)
    }

    pub async fn create_mysql_sinker<'a>(
        url: &str,
        router: &RdbRouter,
        log_level: &str,
        parallel_size: usize,
        batch_size: usize,
    ) -> Result<Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool =
            TaskUtil::create_mysql_conn_pool(url, parallel_size as u32 * 2, enable_sqlx_log)
                .await?;

        let meta_manager = MysqlMetaManager::new(conn_pool.clone()).init().await?;
        let mut sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>> = Vec::new();
        for _ in 0..parallel_size {
            let sinker = MysqlSinker {
                conn_pool: conn_pool.clone(),
                meta_manager: meta_manager.clone(),
                router: router.clone(),
                batch_size,
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    pub async fn create_pg_sinker<'a>(
        url: &str,
        router: &RdbRouter,
        log_level: &str,
        parallel_size: usize,
        batch_size: usize,
    ) -> Result<Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(&log_level);
        let conn_pool =
            TaskUtil::create_pg_conn_pool(url, parallel_size as u32 * 2, enable_sqlx_log).await?;
        let meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;

        let mut sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>> = Vec::new();
        for _ in 0..parallel_size {
            let sinker = PgSinker {
                conn_pool: conn_pool.clone(),
                meta_manager: meta_manager.clone(),
                router: router.clone(),
                batch_size,
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }
}
