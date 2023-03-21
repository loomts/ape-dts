use std::sync::Arc;

use crate::{
    config::{sinker_config::SinkerConfig, task_config::TaskConfig},
    error::Error,
    meta::{mysql::mysql_meta_manager::MysqlMetaManager, pg::pg_meta_manager::PgMetaManager},
    sinker::{
        mysql::{mysql_checker::MysqlChecker, mysql_sinker::MysqlSinker},
        pg::{pg_checker::PgChecker, pg_sinker::PgSinker},
        rdb_router::RdbRouter,
    },
    traits::Sinker,
};

use super::task_util::TaskUtil;

pub struct SinkerUtil {}

impl SinkerUtil {
    pub async fn create_sinkers(
        task_config: &TaskConfig,
    ) -> Result<Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>, Error> {
        let router = RdbRouter::from_config(&task_config.router)?;
        let sinkers = match &task_config.sinker {
            SinkerConfig::Mysql { url, batch_size } => {
                SinkerUtil::create_mysql_sinker(
                    &url,
                    &router,
                    &task_config.runtime.log_level,
                    task_config.pipeline.parallel_size,
                    *batch_size,
                )
                .await?
            }

            SinkerConfig::MysqlCheck { url, batch_size } => {
                SinkerUtil::create_mysql_checker(
                    &url,
                    &router,
                    &task_config.runtime.log_level,
                    task_config.pipeline.parallel_size,
                    *batch_size,
                )
                .await?
            }

            SinkerConfig::Pg { url, batch_size } => {
                SinkerUtil::create_pg_sinker(
                    &url,
                    &router,
                    &task_config.runtime.log_level,
                    task_config.pipeline.parallel_size,
                    *batch_size,
                )
                .await?
            }

            SinkerConfig::PgCheck { url, batch_size } => {
                SinkerUtil::create_pg_checker(
                    &url,
                    &router,
                    &task_config.runtime.log_level,
                    task_config.pipeline.parallel_size,
                    *batch_size,
                )
                .await?
            }
        };
        Ok(sinkers)
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

    pub async fn create_mysql_checker<'a>(
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
            let sinker = MysqlChecker {
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

    pub async fn create_pg_checker<'a>(
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
            let sinker = PgChecker {
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
