use std::{
    str::FromStr,
    sync::{Arc, Mutex, RwLock},
};

use dt_common::{
    config::{
        config_enums::{ConflictPolicyEnum, DbType},
        sinker_config::SinkerConfig,
        task_config::TaskConfig,
    },
    error::Error,
    monitor::monitor::Monitor,
    rdb_filter::RdbFilter,
};
use dt_common::{
    meta::{
        avro::avro_converter::AvroConverter,
        mysql::mysql_meta_manager::MysqlMetaManager,
        pg::pg_meta_manager::PgMetaManager,
        rdb_meta_manager::RdbMetaManager,
        redis::{redis_statistic_type::RedisStatisticType, redis_write_method::RedisWriteMethod},
    },
    utils::redis_util::RedisUtil,
};
use dt_connector::{
    data_marker::DataMarker,
    rdb_router::RdbRouter,
    sinker::{
        dummy_sinker::DummySinker,
        kafka::kafka_sinker::KafkaSinker,
        mongo::{mongo_checker::MongoChecker, mongo_sinker::MongoSinker},
        mysql::{
            mysql_checker::MysqlChecker, mysql_sinker::MysqlSinker,
            mysql_struct_sinker::MysqlStructSinker,
        },
        pg::{pg_checker::PgChecker, pg_sinker::PgSinker, pg_struct_sinker::PgStructSinker},
        redis::{redis_sinker::RedisSinker, redis_statistic_sinker::RedisStatisticSinker},
        starrocks::starrocks_sinker::StarRocksSinker,
    },
    Sinker,
};
use kafka::producer::{Producer, RequiredAcks};
use reqwest::{redirect::Policy, Url};

use super::task_util::TaskUtil;

type Sinkers = Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>;

pub struct SinkerUtil {}

impl SinkerUtil {
    pub async fn create_sinkers(
        task_config: &TaskConfig,
        monitor: Arc<Mutex<Monitor>>,
        data_marker: Option<Arc<RwLock<DataMarker>>>,
    ) -> Result<Sinkers, Error> {
        let sinkers = match &task_config.sinker {
            SinkerConfig::Dummy => {
                Self::create_dummy_sinker(task_config.parallelizer.parallel_size)?
            }

            SinkerConfig::Mysql { url, batch_size } => {
                let router = RdbRouter::from_config(&task_config.router, &DbType::Mysql)?;
                Self::create_mysql_sinker(
                    url,
                    &router,
                    &task_config.runtime.log_level,
                    task_config.parallelizer.parallel_size,
                    *batch_size,
                    monitor,
                    data_marker,
                )
                .await?
            }

            SinkerConfig::MysqlCheck {
                url, batch_size, ..
            } => {
                // checker needs the reverse router
                let router = RdbRouter::from_config(&task_config.router, &DbType::Mysql)?.reverse();
                let filter = RdbFilter::from_config(&task_config.filter, &DbType::Mysql)?;
                let extractor_meta_manager = Self::get_extractor_meta_manager(task_config).await?;
                Self::create_mysql_checker(
                    url,
                    &router,
                    &filter,
                    extractor_meta_manager.unwrap(),
                    &task_config.runtime.log_level,
                    task_config.parallelizer.parallel_size,
                    *batch_size,
                    monitor,
                )
                .await?
            }

            SinkerConfig::Pg { url, batch_size } => {
                let router = RdbRouter::from_config(&task_config.router, &DbType::Pg)?;
                Self::create_pg_sinker(
                    url,
                    &router,
                    &task_config.runtime.log_level,
                    task_config.parallelizer.parallel_size,
                    *batch_size,
                    monitor,
                    data_marker,
                )
                .await?
            }

            SinkerConfig::PgCheck {
                url, batch_size, ..
            } => {
                // checker needs the reverse router
                let router = RdbRouter::from_config(&task_config.router, &DbType::Pg)?.reverse();
                let filter = RdbFilter::from_config(&task_config.filter, &DbType::Pg)?;
                let extractor_meta_manager = Self::get_extractor_meta_manager(task_config).await?;
                Self::create_pg_checker(
                    url,
                    &router,
                    &filter,
                    extractor_meta_manager.unwrap(),
                    &task_config.runtime.log_level,
                    task_config.parallelizer.parallel_size,
                    *batch_size,
                    monitor,
                )
                .await?
            }

            SinkerConfig::Mongo {
                url,
                app_name,
                batch_size,
            } => {
                let router = RdbRouter::from_config(&task_config.router, &DbType::Mongo)?;
                Self::create_mongo_sinker(
                    url,
                    app_name,
                    &router,
                    task_config.parallelizer.parallel_size,
                    *batch_size,
                    monitor,
                )
                .await?
            }

            SinkerConfig::MongoCheck {
                url,
                app_name,
                batch_size,
                ..
            } => {
                let router = RdbRouter::from_config(&task_config.router, &DbType::Mongo)?.reverse();
                Self::create_mongo_checker(
                    url,
                    app_name,
                    &router,
                    task_config.parallelizer.parallel_size,
                    *batch_size,
                    monitor,
                )
                .await?
            }

            SinkerConfig::Kafka {
                url,
                batch_size,
                ack_timeout_secs,
                required_acks,
            } => {
                let router = RdbRouter::from_config(
                    &task_config.router,
                    // use the db_type of extractor
                    &task_config.extractor_basic.db_type,
                )?;
                // kafka sinker may need meta data from RDB extractor
                let meta_manager = Self::get_extractor_meta_manager(task_config).await?;
                let avro_converter = AvroConverter::new(meta_manager);
                Self::create_kafka_sinker(
                    url,
                    &router,
                    task_config.parallelizer.parallel_size,
                    *batch_size,
                    *ack_timeout_secs,
                    required_acks,
                    &avro_converter,
                    monitor,
                )
                .await?
            }

            SinkerConfig::MysqlStruct {
                url,
                conflict_policy,
            } => {
                let filter = RdbFilter::from_config(&task_config.filter, &DbType::Mysql)?;
                Self::create_mysql_struct_sinker(
                    url,
                    &task_config.runtime.log_level,
                    task_config.parallelizer.parallel_size,
                    conflict_policy,
                    &filter,
                )
                .await?
            }

            SinkerConfig::PgStruct {
                url,
                conflict_policy,
            } => {
                let filter = RdbFilter::from_config(&task_config.filter, &DbType::Pg)?;
                Self::create_pg_struct_sinker(
                    url,
                    &task_config.runtime.log_level,
                    task_config.parallelizer.parallel_size,
                    conflict_policy,
                    &filter,
                )
                .await?
            }

            SinkerConfig::Redis {
                url,
                batch_size,
                method,
                is_cluster,
            } => {
                // redis sinker may need meta data from RDB extractor
                let meta_manager = Self::get_extractor_meta_manager(task_config).await?;
                Self::create_redis_sinker(
                    url,
                    task_config.parallelizer.parallel_size,
                    *batch_size,
                    method,
                    meta_manager,
                    monitor,
                    data_marker,
                    *is_cluster,
                )
                .await?
            }

            SinkerConfig::RedisStatistic {
                statistic_type,
                data_size_threshold,
                freq_threshold,
                ..
            } => {
                Self::create_redis_statistic_sinker(
                    task_config.parallelizer.parallel_size,
                    statistic_type,
                    *freq_threshold,
                    *data_size_threshold,
                    monitor,
                )
                .await?
            }

            SinkerConfig::Starrocks {
                batch_size,
                stream_load_url,
                ..
            } => {
                Self::create_starrocks_sinker(
                    stream_load_url,
                    task_config.parallelizer.parallel_size,
                    *batch_size,
                    monitor,
                )
                .await?
            }
        };
        Ok(sinkers)
    }

    fn create_dummy_sinker(parallel_size: usize) -> Result<Sinkers, Error> {
        let mut sub_sinkers: Sinkers = Vec::new();
        for _ in 0..parallel_size {
            let sinker = DummySinker {};
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_mysql_sinker<'a>(
        url: &str,
        router: &RdbRouter,
        log_level: &str,
        parallel_size: usize,
        batch_size: usize,
        monitor: Arc<Mutex<Monitor>>,
        data_marker: Option<Arc<RwLock<DataMarker>>>,
    ) -> Result<Sinkers, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool =
            TaskUtil::create_mysql_conn_pool(url, parallel_size as u32 * 2, enable_sqlx_log)
                .await?;

        let meta_manager = MysqlMetaManager::new(conn_pool.clone()).init().await?;
        let mut sub_sinkers: Sinkers = Vec::new();
        // to avoid contention for monitor write lock between sinker threads,
        // create a monitor for each sinker instead of sharing a single monitor between sinkers,
        // sometimes a sinker may cost several millis to get write lock for a global monitor.
        for _ in 0..parallel_size {
            let sinker = MysqlSinker {
                url: url.to_string(),
                conn_pool: conn_pool.clone(),
                meta_manager: meta_manager.clone(),
                router: router.clone(),
                batch_size,
                monitor: monitor.clone(),
                data_marker: data_marker.clone(),
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_mysql_checker<'a>(
        url: &str,
        router: &RdbRouter,
        filter: &RdbFilter,
        extractor_meta_manager: RdbMetaManager,
        log_level: &str,
        parallel_size: usize,
        batch_size: usize,
        monitor: Arc<Mutex<Monitor>>,
    ) -> Result<Sinkers, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool =
            TaskUtil::create_mysql_conn_pool(url, parallel_size as u32 * 2, enable_sqlx_log)
                .await?;

        let meta_manager = MysqlMetaManager::new(conn_pool.clone()).init().await?;
        let mut sub_sinkers: Sinkers = Vec::new();
        for _ in 0..parallel_size {
            let sinker = MysqlChecker {
                conn_pool: conn_pool.clone(),
                meta_manager: meta_manager.clone(),
                extractor_meta_manager: extractor_meta_manager.clone(),
                router: router.clone(),
                filter: filter.clone(),
                batch_size,
                monitor: monitor.clone(),
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
        monitor: Arc<Mutex<Monitor>>,
        data_marker: Option<Arc<RwLock<DataMarker>>>,
    ) -> Result<Sinkers, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool =
            TaskUtil::create_pg_conn_pool(url, parallel_size as u32 * 2, enable_sqlx_log).await?;
        let meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;

        let mut sub_sinkers: Sinkers = Vec::new();
        for _ in 0..parallel_size {
            let sinker = PgSinker {
                url: url.to_string(),
                conn_pool: conn_pool.clone(),
                meta_manager: meta_manager.clone(),
                router: router.clone(),
                batch_size,
                monitor: monitor.clone(),
                data_marker: data_marker.clone(),
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_pg_checker<'a>(
        url: &str,
        router: &RdbRouter,
        filter: &RdbFilter,
        extractor_meta_manager: RdbMetaManager,
        log_level: &str,
        parallel_size: usize,
        batch_size: usize,
        monitor: Arc<Mutex<Monitor>>,
    ) -> Result<Sinkers, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool =
            TaskUtil::create_pg_conn_pool(url, parallel_size as u32 * 2, enable_sqlx_log).await?;
        let meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;

        let mut sub_sinkers: Sinkers = Vec::new();
        for _ in 0..parallel_size {
            let sinker = PgChecker {
                conn_pool: conn_pool.clone(),
                meta_manager: meta_manager.clone(),
                extractor_meta_manager: extractor_meta_manager.clone(),
                router: router.clone(),
                filter: filter.clone(),
                batch_size,
                monitor: monitor.clone(),
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_kafka_sinker<'a>(
        url: &str,
        router: &RdbRouter,
        parallel_size: usize,
        batch_size: usize,
        ack_timeout_secs: u64,
        required_acks: &str,
        avro_converter: &AvroConverter,
        monitor: Arc<Mutex<Monitor>>,
    ) -> Result<Sinkers, Error> {
        let brokers = vec![url.to_string()];
        let acks = match required_acks {
            "all" => RequiredAcks::All,
            "none" => RequiredAcks::None,
            _ => RequiredAcks::One,
        };

        let mut sub_sinkers: Sinkers = Vec::new();
        for _ in 0..parallel_size {
            // TODO, authentication, https://github.com/kafka-rust/kafka-rust/blob/master/examples/example-ssl.rs

            let producer = Producer::from_hosts(brokers.clone())
                .with_ack_timeout(std::time::Duration::from_secs(ack_timeout_secs))
                .with_required_acks(acks)
                .create()
                .unwrap();

            let sinker = KafkaSinker {
                batch_size,
                router: router.clone(),
                producer,
                avro_converter: avro_converter.clone(),
                monitor: monitor.clone(),
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_mongo_sinker<'a>(
        url: &str,
        app_name: &str,
        router: &RdbRouter,
        parallel_size: usize,
        batch_size: usize,
        monitor: Arc<Mutex<Monitor>>,
    ) -> Result<Sinkers, Error> {
        let mut sub_sinkers: Sinkers = Vec::new();
        for _ in 0..parallel_size {
            let mongo_client = TaskUtil::create_mongo_client(url, app_name).await.unwrap();
            let sinker = MongoSinker {
                batch_size,
                router: router.clone(),
                mongo_client,
                monitor: monitor.clone(),
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_mongo_checker<'a>(
        url: &str,
        app_name: &str,
        router: &RdbRouter,
        parallel_size: usize,
        batch_size: usize,
        monitor: Arc<Mutex<Monitor>>,
    ) -> Result<Sinkers, Error> {
        let mut sub_sinkers: Sinkers = Vec::new();
        for _ in 0..parallel_size {
            let mongo_client = TaskUtil::create_mongo_client(url, app_name).await.unwrap();
            let sinker = MongoChecker {
                batch_size,
                router: router.clone(),
                mongo_client,
                monitor: monitor.clone(),
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_mysql_struct_sinker<'a>(
        url: &str,
        log_level: &str,
        parallel_size: usize,
        conflict_policy: &ConflictPolicyEnum,
        filter: &RdbFilter,
    ) -> Result<Sinkers, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool =
            TaskUtil::create_mysql_conn_pool(url, parallel_size as u32 * 2, enable_sqlx_log)
                .await?;

        let mut sub_sinkers: Sinkers = Vec::new();
        for _ in 0..parallel_size {
            let sinker = MysqlStructSinker {
                conn_pool: conn_pool.clone(),
                conflict_policy: conflict_policy.clone(),
                filter: filter.clone(),
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_pg_struct_sinker<'a>(
        url: &str,
        log_level: &str,
        parallel_size: usize,
        conflict_policy: &ConflictPolicyEnum,
        filter: &RdbFilter,
    ) -> Result<Sinkers, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool =
            TaskUtil::create_pg_conn_pool(url, parallel_size as u32 * 2, enable_sqlx_log).await?;

        let mut sub_sinkers: Sinkers = Vec::new();
        for _ in 0..parallel_size {
            let sinker = PgStructSinker {
                conn_pool: conn_pool.clone(),
                conflict_policy: conflict_policy.clone(),
                filter: filter.clone(),
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_redis_sinker<'a>(
        url: &str,
        parallel_size: usize,
        batch_size: usize,
        method: &str,
        meta_manager: Option<RdbMetaManager>,
        monitor: Arc<Mutex<Monitor>>,
        data_marker: Option<Arc<RwLock<DataMarker>>>,
        is_cluster: bool,
    ) -> Result<Sinkers, Error> {
        let mut sub_sinkers: Sinkers = Vec::new();

        let mut conn = RedisUtil::create_redis_conn(url).await?;
        let version = RedisUtil::get_redis_version(&mut conn)?;
        let method = RedisWriteMethod::from_str(method).unwrap();

        if is_cluster {
            let url_info = Url::parse(url).unwrap();
            let username = url_info.username();
            let password = url_info.password().unwrap_or("").to_string();

            let nodes = RedisUtil::get_cluster_master_nodes(&mut conn)?;
            for node in nodes.iter() {
                if !node.is_master {
                    continue;
                }

                let new_url = format!("redis://{}:{}@{}", username, password, node.address);
                let conn = RedisUtil::create_redis_conn(&new_url).await?;
                let sinker = RedisSinker {
                    id: node.address.clone(),
                    conn,
                    batch_size,
                    now_db_id: -1,
                    version,
                    method: method.clone(),
                    meta_manager: meta_manager.clone(),
                    monitor: monitor.clone(),
                    data_marker: data_marker.clone(),
                };
                sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
            }
        } else {
            for _ in 0..parallel_size {
                let conn = RedisUtil::create_redis_conn(url).await?;
                let sinker = RedisSinker {
                    id: url.to_string(),
                    conn,
                    batch_size,
                    now_db_id: -1,
                    version,
                    method: method.clone(),
                    meta_manager: meta_manager.clone(),
                    monitor: monitor.clone(),
                    data_marker: data_marker.clone(),
                };
                sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
            }
        }

        Ok(sub_sinkers)
    }

    async fn create_redis_statistic_sinker(
        parallel_size: usize,
        statistic_type: &str,
        freq_threshold: i64,
        data_size_threshold: usize,
        monitor: Arc<Mutex<Monitor>>,
    ) -> Result<Sinkers, Error> {
        let statistic_type = RedisStatisticType::from_str(statistic_type).unwrap();
        let mut sub_sinkers: Sinkers = Vec::new();
        for _ in 0..parallel_size {
            let sinker = RedisStatisticSinker {
                statistic_type: statistic_type.clone(),
                data_size_threshold,
                freq_threshold,
                monitor: monitor.clone(),
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn create_starrocks_sinker<'a>(
        stream_load_url: &str,
        parallel_size: usize,
        batch_size: usize,
        monitor: Arc<Mutex<Monitor>>,
    ) -> Result<Sinkers, Error> {
        let mut sub_sinkers: Sinkers = Vec::new();
        for _ in 0..parallel_size {
            let url_info = Url::parse(stream_load_url).unwrap();
            let host = url_info.host_str().unwrap().to_string();
            let port = format!("{}", url_info.port().unwrap());
            let username = url_info.username().to_string();
            let password = url_info.password().unwrap_or("").to_string();

            let custom = Policy::custom(|attempt| attempt.follow());
            let client = reqwest::Client::builder()
                .http1_title_case_headers()
                .redirect(custom)
                .build()
                .unwrap();

            let sinker = StarRocksSinker {
                client,
                host,
                port,
                username,
                password,
                batch_size,
                monitor: monitor.clone(),
            };
            sub_sinkers.push(Arc::new(async_mutex::Mutex::new(Box::new(sinker))));
        }
        Ok(sub_sinkers)
    }

    async fn get_extractor_meta_manager(
        task_config: &TaskConfig,
    ) -> Result<Option<RdbMetaManager>, Error> {
        let extractor_url = &task_config.extractor_basic.url;
        let meta_manager = match task_config.extractor_basic.db_type {
            DbType::Mysql => {
                let conn_pool = TaskUtil::create_mysql_conn_pool(extractor_url, 1, true).await?;
                let meta_manager = MysqlMetaManager::new(conn_pool.clone()).init().await?;
                Some(RdbMetaManager::from_mysql(meta_manager))
            }
            DbType::Pg => {
                let conn_pool = TaskUtil::create_pg_conn_pool(extractor_url, 1, true).await?;
                let meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;
                Some(RdbMetaManager::from_pg(meta_manager))
            }
            _ => None,
        };
        Ok(meta_manager)
    }
}
