use std::{str::FromStr, sync::Arc, time::Duration};

use dt_common::config::{sinker_config::SinkerConfig, task_config::TaskConfig};
use kafka::producer::{Producer, RequiredAcks};
use reqwest::Client;
use rusoto_core::Region;
use rusoto_s3::S3Client;

use crate::{
    error::Error,
    meta::{mysql::mysql_meta_manager::MysqlMetaManager, pg::pg_meta_manager::PgMetaManager},
    sinker::{
        foxlake_sinker::FoxlakeSinker,
        kafka::kafka_router::KafkaRouter,
        kafka::kafka_sinker::KafkaSinker,
        mysql::{mysql_checker::MysqlChecker, mysql_sinker::MysqlSinker},
        open_faas_sinker::OpenFaasSinker,
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
        let sinkers = match &task_config.sinker {
            SinkerConfig::Mysql { url, batch_size } => {
                let router = RdbRouter::from_config(&task_config.router)?;
                SinkerUtil::create_mysql_sinker(
                    &url,
                    &router,
                    &task_config.runtime.log_level,
                    task_config.pipeline.parallel_size,
                    *batch_size,
                )
                .await?
            }

            SinkerConfig::MysqlCheck {
                url, batch_size, ..
            } => {
                let router = RdbRouter::from_config(&task_config.router)?;
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
                let router = RdbRouter::from_config(&task_config.router)?;
                SinkerUtil::create_pg_sinker(
                    &url,
                    &router,
                    &task_config.runtime.log_level,
                    task_config.pipeline.parallel_size,
                    *batch_size,
                )
                .await?
            }

            SinkerConfig::PgCheck {
                url, batch_size, ..
            } => {
                let router = RdbRouter::from_config(&task_config.router)?;
                SinkerUtil::create_pg_checker(
                    &url,
                    &router,
                    &task_config.runtime.log_level,
                    task_config.pipeline.parallel_size,
                    *batch_size,
                )
                .await?
            }

            SinkerConfig::Kafka {
                url,
                batch_size,
                ack_timeout_secs,
                required_acks,
            } => {
                let router = KafkaRouter::from_config(&task_config.router)?;
                SinkerUtil::create_kafka_sinker(
                    &url,
                    &router,
                    task_config.pipeline.parallel_size,
                    *batch_size,
                    *ack_timeout_secs,
                    required_acks,
                )
                .await?
            }

            SinkerConfig::OpenFaas {
                url,
                batch_size,
                timeout_secs,
            } => {
                SinkerUtil::create_open_faas_sinker(
                    &url,
                    task_config.pipeline.parallel_size,
                    *batch_size,
                    *timeout_secs,
                )
                .await?
            }

            SinkerConfig::Foxlake {
                batch_size,
                bucket,
                access_key,
                secret_key,
                region,
                root_dir,
            } => {
                SinkerUtil::create_foxlake_sinker(
                    task_config.pipeline.parallel_size,
                    *batch_size,
                    bucket,
                    root_dir,
                    access_key,
                    secret_key,
                    region,
                )
                .await?
            }

            SinkerConfig::BasicConfig { .. } => {
                return Err(Error::Unexpected {
                    error: "unexpected sinker type".to_string(),
                });
            }
        };
        Ok(sinkers)
    }

    async fn create_mysql_sinker<'a>(
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

    async fn create_mysql_checker<'a>(
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

    async fn create_pg_sinker<'a>(
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

    async fn create_pg_checker<'a>(
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

    async fn create_kafka_sinker<'a>(
        url: &str,
        router: &KafkaRouter,
        parallel_size: usize,
        batch_size: usize,
        ack_timeout_secs: u64,
        required_acks: &str,
    ) -> Result<Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>, Error> {
        let brokers = vec![url.to_string()];
        let acks = match required_acks {
            "all" => RequiredAcks::All,
            "none" => RequiredAcks::None,
            _ => RequiredAcks::One,
        };

        let mut sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>> = Vec::new();
        for _ in 0..parallel_size {
            // TODO, authentication, https://github.com/kafka-rust/kafka-rust/blob/master/examples/example-ssl.rs
            let producer = Producer::from_hosts(brokers.clone())
                .with_ack_timeout(std::time::Duration::from_secs(ack_timeout_secs as u64))
                .with_required_acks(acks)
                .create()
                .unwrap();
            let sinker = KafkaSinker {
                batch_size,
                kafka_router: router.clone(),
                producer,
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_open_faas_sinker<'a>(
        url: &str,
        parallel_size: usize,
        batch_size: usize,
        timeout_secs: u64,
    ) -> Result<Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>, Error> {
        let mut sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>> = Vec::new();
        for _ in 0..parallel_size {
            let http_client = Client::builder()
                .connect_timeout(Duration::from_secs(timeout_secs))
                .build()
                .unwrap();
            let sinker = OpenFaasSinker {
                batch_size,
                http_client,
                gateway_url: url.to_string(),
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_foxlake_sinker<'a>(
        parallel_size: usize,
        batch_size: usize,
        bucket: &str,
        root_dir: &str,
        access_key: &str,
        secret_key: &str,
        region: &str,
    ) -> Result<Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>, Error> {
        let mut sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>> = Vec::new();
        for _ in 0..parallel_size {
            let region = Region::from_str(region).unwrap();
            let credentials = rusoto_credential::StaticProvider::new_minimal(
                access_key.to_owned(),
                secret_key.to_owned(),
            );
            let s3_client =
                S3Client::new_with(rusoto_core::HttpClient::new().unwrap(), credentials, region);

            let sinker = FoxlakeSinker {
                batch_size,
                bucket: bucket.to_string(),
                root_dir: root_dir.to_string(),
                s3_client,
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }
}
