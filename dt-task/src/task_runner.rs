use std::{
    fs::{self, File},
    io::Read,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    time::Duration,
};

use concurrent_queue::ConcurrentQueue;
use dt_common::{
    config::{
        config_enums::DbType, config_token_parser::ConfigTokenParser,
        extractor_config::ExtractorConfig, sinker_config::SinkerConfig, task_config::TaskConfig,
    },
    error::Error,
    monitor::monitor::Monitor,
    utils::{rdb_filter::RdbFilter, sql_util::SqlUtil, time_util::TimeUtil},
};
use dt_connector::{
    data_marker::DataMarker,
    extractor::{
        base_extractor::BaseExtractor, extractor_monitor::ExtractorMonitor,
        snapshot_resumer::SnapshotResumer,
    },
    rdb_router::RdbRouter,
    Extractor, Sinker,
};
use dt_meta::{dt_data::DtItem, position::Position, row_type::RowType, syncer::Syncer};
use dt_pipeline::{base_pipeline::BasePipeline, Pipeline};

use log4rs::config::RawConfig;
use ratelimit::Ratelimiter;
use tokio::try_join;

use crate::task_util::TaskUtil;

use super::{
    extractor_util::ExtractorUtil, parallelizer_util::ParallelizerUtil, sinker_util::SinkerUtil,
};

pub struct TaskRunner {
    config: TaskConfig,
}

const CHECK_LOG_DIR_PLACEHODLER: &str = "CHECK_LOG_DIR_PLACEHODLER";
const STATISTIC_LOG_DIR_PLACEHODLER: &str = "STATISTIC_LOG_DIR_PLACEHODLER";
const LOG_LEVEL_PLACEHODLER: &str = "LOG_LEVEL_PLACEHODLER";
const LOG_DIR_PLACEHODLER: &str = "LOG_DIR_PLACEHODLER";
const DEFAULT_CHECK_LOG_DIR_PLACEHODLER: &str = "LOG_DIR_PLACEHODLER/check";
const DEFAULT_STATISTIC_LOG_DIR_PLACEHODLER: &str = "LOG_DIR_PLACEHODLER/statistic";

impl TaskRunner {
    pub async fn new(task_config_file: String) -> Self {
        Self {
            config: TaskConfig::new(&task_config_file),
        }
    }

    pub async fn start_task(&self, enable_log4rs: bool) -> Result<(), Error> {
        if enable_log4rs {
            self.init_log4rs()?;
        }

        match &self.config.extractor {
            ExtractorConfig::MysqlStruct { url, .. }
            | ExtractorConfig::PgStruct { url, .. }
            | ExtractorConfig::MysqlSnapshot { url, .. }
            | ExtractorConfig::PgSnapshot { url, .. }
            | ExtractorConfig::MongoSnapshot { url, .. } => self.start_multi_task(url).await?,

            _ => self.start_single_task(&self.config.extractor).await?,
        };

        Ok(())
    }

    async fn start_multi_task(&self, url: &str) -> Result<(), Error> {
        let db_type = self.config.extractor_basic.db_type.clone();
        let mut filter = RdbFilter::from_config(&self.config.filter, db_type.clone())?;
        let dbs = TaskUtil::list_dbs(url, &db_type).await?;
        for db in dbs.iter() {
            if filter.filter_db(db) {
                continue;
            }

            // start a task for each db
            let db_extractor_config = match &self.config.extractor {
                ExtractorConfig::MysqlStruct { url, .. } => Some(ExtractorConfig::MysqlStruct {
                    url: url.clone(),
                    db: db.clone(),
                }),

                ExtractorConfig::PgStruct { url, .. } => Some(ExtractorConfig::PgStruct {
                    url: url.clone(),
                    schema: db.clone(),
                }),

                _ => None,
            };

            if let Some(extractor_config) = db_extractor_config {
                self.start_single_task(&extractor_config).await?;
                continue;
            }

            // start a task for each tb
            let tbs = TaskUtil::list_tbs(url, db, &db_type).await?;
            for tb in tbs.iter() {
                if filter.filter_event(db, tb, &RowType::Insert.to_string()) {
                    continue;
                }

                let tb_extractor_config = match &self.config.extractor {
                    ExtractorConfig::MysqlSnapshot {
                        url,
                        sample_interval,
                        ..
                    } => ExtractorConfig::MysqlSnapshot {
                        url: url.clone(),
                        db: db.clone(),
                        tb: tb.clone(),
                        sample_interval: *sample_interval,
                    },

                    ExtractorConfig::PgSnapshot {
                        url,
                        sample_interval,
                        ..
                    } => ExtractorConfig::PgSnapshot {
                        url: url.clone(),
                        schema: db.clone(),
                        tb: tb.clone(),
                        sample_interval: *sample_interval,
                    },

                    ExtractorConfig::MongoSnapshot { url, app_name, .. } => {
                        ExtractorConfig::MongoSnapshot {
                            url: url.clone(),
                            app_name: app_name.clone(),
                            db: db.clone(),
                            tb: tb.clone(),
                        }
                    }

                    _ => {
                        return Err(Error::ConfigError("unsupported extractor config".into()));
                    }
                };

                self.start_single_task(&tb_extractor_config).await?;
            }
        }
        Ok(())
    }

    async fn start_single_task(&self, extractor_config: &ExtractorConfig) -> Result<(), Error> {
        let buffer = Arc::new(ConcurrentQueue::bounded(self.config.pipeline.buffer_size));
        let shut_down = Arc::new(AtomicBool::new(false));
        let syncer = Arc::new(Mutex::new(Syncer {
            received_position: Position::None,
            committed_position: Position::None,
        }));

        let (extractor_data_marker, sinker_data_marker) = if let Some(data_marker_config) =
            &self.config.data_marker
        {
            let extractor_data_marker =
                DataMarker::from_config(data_marker_config, &self.config.extractor_basic.db_type)
                    .unwrap();
            let sinker_data_marker =
                DataMarker::from_config(data_marker_config, &self.config.sinker_basic.db_type)
                    .unwrap();
            (Some(extractor_data_marker), Some(sinker_data_marker))
        } else {
            (None, None)
        };
        let rw_sinker_data_marker = sinker_data_marker
            .clone()
            .map(|data_marker| Arc::new(RwLock::new(data_marker)));

        // extractor
        let monitor_time_window_secs = self.config.pipeline.checkpoint_interval_secs as usize;
        let monitor_count_window = self.config.pipeline.buffer_size;
        let extractor_monitor = Arc::new(Mutex::new(Monitor::new(
            "extractor",
            monitor_time_window_secs,
            monitor_count_window,
        )));
        let mut extractor = self
            .create_extractor(
                extractor_config,
                buffer.clone(),
                shut_down.clone(),
                syncer.clone(),
                extractor_monitor.clone(),
                extractor_data_marker,
            )
            .await?;

        // sinkers
        let sinker_monitor = Arc::new(Mutex::new(Monitor::new(
            "sinker",
            monitor_time_window_secs,
            monitor_count_window,
        )));
        let sinkers = SinkerUtil::create_sinkers(
            &self.config,
            sinker_monitor.clone(),
            rw_sinker_data_marker.clone(),
        )
        .await?;

        // pipeline
        let pipeline_monitor = Arc::new(Mutex::new(Monitor::new(
            "pipeline",
            monitor_time_window_secs,
            monitor_count_window,
        )));
        let mut pipeline = self
            .create_pipeline(
                buffer,
                shut_down.clone(),
                syncer,
                sinkers,
                pipeline_monitor.clone(),
                rw_sinker_data_marker.clone(),
            )
            .await?;

        // do pre operations before task starts
        self.pre_single_task(sinker_data_marker).await.unwrap();

        // start threads
        let f1 = tokio::spawn(async move {
            extractor.extract().await.unwrap();
            extractor.close().await.unwrap();
        });

        let f2 = tokio::spawn(async move {
            pipeline.start().await.unwrap();
            pipeline.stop().await.unwrap();
        });

        let interval_secs = self.config.pipeline.checkpoint_interval_secs;
        let f3 = tokio::spawn(async move {
            Self::flush_monitors(
                interval_secs,
                shut_down,
                extractor_monitor,
                pipeline_monitor,
                sinker_monitor,
            )
            .await
        });
        try_join!(f1, f2, f3).unwrap();
        Ok(())
    }

    async fn create_pipeline(
        &self,
        buffer: Arc<ConcurrentQueue<DtItem>>,
        shut_down: Arc<AtomicBool>,
        syncer: Arc<Mutex<Syncer>>,
        sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
        monitor: Arc<Mutex<Monitor>>,
        data_marker: Option<Arc<RwLock<DataMarker>>>,
    ) -> Result<Box<dyn Pipeline + Send>, Error> {
        let rps_limiter = if self.config.pipeline.max_rps > 0 {
            Some(
                Ratelimiter::builder(self.config.pipeline.max_rps, Duration::from_secs(1))
                    .max_tokens(self.config.pipeline.max_rps)
                    .initial_available(self.config.pipeline.max_rps)
                    .build()
                    .unwrap(),
            )
        } else {
            None
        };
        let parallelizer =
            ParallelizerUtil::create_parallelizer(&self.config, monitor.clone(), rps_limiter)
                .await?;
        let pipeline = BasePipeline {
            buffer,
            parallelizer,
            sinker_basic_config: self.config.sinker_basic.clone(),
            sinkers,
            shut_down,
            checkpoint_interval_secs: self.config.pipeline.checkpoint_interval_secs,
            batch_sink_interval_secs: self.config.pipeline.batch_sink_interval_secs,
            syncer,
            monitor,
            data_marker,
        };

        Ok(Box::new(pipeline))
    }

    async fn create_extractor(
        &self,
        extractor_config: &ExtractorConfig,
        buffer: Arc<ConcurrentQueue<DtItem>>,
        shut_down: Arc<AtomicBool>,
        syncer: Arc<Mutex<Syncer>>,
        monitor: Arc<Mutex<Monitor>>,
        data_marker: Option<DataMarker>,
    ) -> Result<Box<dyn Extractor + Send>, Error> {
        let resumer =
            SnapshotResumer::new(&self.config.extractor_basic.db_type, &self.config.resumer)?;
        let router =
            RdbRouter::from_config(&self.config.router, &self.config.extractor_basic.db_type)?;

        let base_extractor = BaseExtractor {
            buffer,
            router,
            shut_down,
            monitor: ExtractorMonitor::new(monitor),
            data_marker,
        };

        let extractor: Box<dyn Extractor + Send> = match extractor_config {
            ExtractorConfig::MysqlSnapshot {
                url,
                db,
                tb,
                sample_interval,
            } => {
                let extractor = ExtractorUtil::create_mysql_snapshot_extractor(
                    base_extractor,
                    url,
                    db,
                    tb,
                    self.config.pipeline.buffer_size,
                    *sample_interval,
                    resumer.clone(),
                    &self.config.runtime.log_level,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::MysqlCheck {
                url,
                check_log_dir,
                batch_size,
            } => {
                let extractor = ExtractorUtil::create_mysql_check_extractor(
                    base_extractor,
                    url,
                    check_log_dir,
                    *batch_size,
                    &self.config.runtime.log_level,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::MysqlCdc {
                url,
                binlog_filename,
                binlog_position,
                server_id,
                heartbeat_interval_secs,
                heartbeat_tb,
            } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Mysql)?;
                let extractor = ExtractorUtil::create_mysql_cdc_extractor(
                    base_extractor,
                    url,
                    binlog_filename,
                    *binlog_position,
                    *server_id,
                    *heartbeat_interval_secs,
                    heartbeat_tb,
                    filter,
                    &self.config.runtime.log_level,
                    syncer,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::PgSnapshot {
                url,
                schema: db,
                tb,
                sample_interval,
            } => {
                let extractor = ExtractorUtil::create_pg_snapshot_extractor(
                    base_extractor,
                    url,
                    db,
                    tb,
                    self.config.pipeline.buffer_size,
                    *sample_interval,
                    resumer.clone(),
                    &self.config.runtime.log_level,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::PgCheck {
                url,
                check_log_dir,
                batch_size,
            } => {
                let extractor = ExtractorUtil::create_pg_check_extractor(
                    base_extractor,
                    url,
                    check_log_dir,
                    *batch_size,
                    &self.config.runtime.log_level,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::PgCdc {
                url,
                slot_name,
                pub_name,
                start_lsn,
                keepalive_interval_secs,
                heartbeat_interval_secs,
                heartbeat_tb,
                ddl_command_tb,
            } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Pg)?;
                let extractor = ExtractorUtil::create_pg_cdc_extractor(
                    base_extractor,
                    url,
                    slot_name,
                    pub_name,
                    start_lsn,
                    *keepalive_interval_secs,
                    *heartbeat_interval_secs,
                    heartbeat_tb,
                    filter,
                    &self.config.runtime.log_level,
                    ddl_command_tb,
                    syncer,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::MongoSnapshot {
                url,
                app_name,
                db,
                tb,
            } => {
                let extractor = ExtractorUtil::create_mongo_snapshot_extractor(
                    base_extractor,
                    url,
                    app_name,
                    db,
                    tb,
                    resumer.clone(),
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::MongoCdc {
                url,
                app_name,
                resume_token,
                start_timestamp,
                source,
                heartbeat_interval_secs,
                heartbeat_tb,
            } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Mongo)?;
                let extractor = ExtractorUtil::create_mongo_cdc_extractor(
                    base_extractor,
                    url,
                    app_name,
                    resume_token,
                    start_timestamp,
                    source,
                    filter,
                    *heartbeat_interval_secs,
                    heartbeat_tb,
                    syncer,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::MongoCheck {
                url,
                app_name,
                check_log_dir,
                batch_size,
            } => {
                let extractor = ExtractorUtil::create_mongo_check_extractor(
                    base_extractor,
                    url,
                    app_name,
                    check_log_dir,
                    *batch_size,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::MysqlStruct { url, db } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Mysql)?;
                let extractor = ExtractorUtil::create_mysql_struct_extractor(
                    base_extractor,
                    url,
                    db,
                    filter,
                    &self.config.runtime.log_level,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::PgStruct { url, schema } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Pg)?;
                let extractor = ExtractorUtil::create_pg_struct_extractor(
                    base_extractor,
                    url,
                    schema,
                    filter,
                    &self.config.runtime.log_level,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::RedisSnapshot { url, repl_port } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Redis)?;
                let extractor = ExtractorUtil::create_redis_snapshot_extractor(
                    base_extractor,
                    url,
                    *repl_port,
                    filter,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::RedisSnapshotFile { file_path } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Redis)?;
                let extractor = ExtractorUtil::create_redis_snapshot_file_extractor(
                    base_extractor,
                    file_path,
                    filter,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::RedisCdc {
                url,
                repl_id,
                repl_offset,
                now_db_id,
                repl_port,
                keepalive_interval_secs,
                heartbeat_interval_secs,
                heartbeat_key,
            } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Redis)?;
                let extractor = ExtractorUtil::create_redis_cdc_extractor(
                    base_extractor,
                    url,
                    repl_id,
                    *repl_offset,
                    *repl_port,
                    *now_db_id,
                    *keepalive_interval_secs,
                    *heartbeat_interval_secs,
                    heartbeat_key,
                    filter,
                    syncer,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::Kafka {
                url,
                group,
                topic,
                partition,
                offset,
                ack_interval_secs,
            } => {
                let meta_manager = TaskUtil::create_rdb_meta_manager(&self.config).await?;
                let extractor = ExtractorUtil::create_kafka_extractor(
                    base_extractor,
                    url,
                    group,
                    topic,
                    *partition,
                    *offset,
                    *ack_interval_secs,
                    meta_manager,
                    syncer,
                )
                .await?;
                Box::new(extractor)
            }
        };
        Ok(extractor)
    }

    fn init_log4rs(&self) -> Result<(), Error> {
        let log4rs_file = &self.config.runtime.log4rs_file;
        if fs::metadata(log4rs_file).is_err() {
            return Ok(());
        }

        let mut config_str = String::new();
        File::open(log4rs_file)?.read_to_string(&mut config_str)?;

        match &self.config.sinker {
            SinkerConfig::MysqlCheck { check_log_dir, .. }
            | SinkerConfig::PgCheck { check_log_dir, .. } => {
                if !check_log_dir.is_empty() {
                    config_str = config_str.replace(CHECK_LOG_DIR_PLACEHODLER, check_log_dir);
                }
            }

            SinkerConfig::RedisStatistic {
                statistic_log_dir, ..
            } => {
                if !statistic_log_dir.is_empty() {
                    config_str =
                        config_str.replace(STATISTIC_LOG_DIR_PLACEHODLER, statistic_log_dir);
                }
            }

            _ => {}
        }

        config_str = config_str
            .replace(CHECK_LOG_DIR_PLACEHODLER, DEFAULT_CHECK_LOG_DIR_PLACEHODLER)
            .replace(
                STATISTIC_LOG_DIR_PLACEHODLER,
                DEFAULT_STATISTIC_LOG_DIR_PLACEHODLER,
            )
            .replace(LOG_DIR_PLACEHODLER, &self.config.runtime.log_dir)
            .replace(LOG_LEVEL_PLACEHODLER, &self.config.runtime.log_level);

        let config: RawConfig = serde_yaml::from_str(&config_str)?;
        log4rs::init_raw_config(config).unwrap();
        Ok(())
    }

    async fn flush_monitors(
        interval_secs: u64,
        shut_down: Arc<AtomicBool>,
        extractor_monitor: Arc<Mutex<Monitor>>,
        pipeline_monitor: Arc<Mutex<Monitor>>,
        sinker_monitor: Arc<Mutex<Monitor>>,
    ) {
        loop {
            // do an extra flush before exit if task finished
            let finished = shut_down.load(Ordering::Acquire);
            if !finished {
                TimeUtil::sleep_millis(interval_secs * 1000).await;
            }

            extractor_monitor.lock().unwrap().flush();
            pipeline_monitor.lock().unwrap().flush();
            sinker_monitor.lock().unwrap().flush();

            if finished {
                break;
            }
        }
    }

    async fn pre_single_task(&self, sinker_data_marker: Option<DataMarker>) -> Result<(), Error> {
        // create heartbeat table
        let db_tb = match &self.config.extractor {
            ExtractorConfig::MysqlCdc { heartbeat_tb, .. }
            | ExtractorConfig::PgCdc { heartbeat_tb, .. } => ConfigTokenParser::parse(
                heartbeat_tb,
                &['.'],
                &SqlUtil::get_escape_pairs(&self.config.extractor_basic.db_type),
            ),
            _ => vec![],
        };

        if db_tb.len() == 2 {
            match &self.config.extractor {
                ExtractorConfig::MysqlCdc { url, .. } => {
                    let db_sql = format!("CREATE DATABASE IF NOT EXISTS `{}`", db_tb[0]);
                    let tb_sql = format!(
                        "CREATE TABLE IF NOT EXISTS `{}`.`{}`(
                        server_id INT UNSIGNED,
                        update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        received_binlog_filename VARCHAR(255),
                        received_next_event_position INT UNSIGNED,
                        received_timestamp VARCHAR(255),
                        flushed_binlog_filename VARCHAR(255),
                        flushed_next_event_position INT UNSIGNED,
                        flushed_timestamp VARCHAR(255),
                        PRIMARY KEY(server_id)
                    )",
                        db_tb[0], db_tb[1]
                    );

                    if !TaskUtil::check_tb_exist(url, &db_tb[0], &db_tb[1], &DbType::Mysql).await {
                        let conn_pool = TaskUtil::create_mysql_conn_pool(url, 1, true)
                            .await
                            .unwrap();
                        let db_query = sqlx::query(&db_sql);
                        db_query.execute(&conn_pool).await.unwrap();
                        let tb_query = sqlx::query(&tb_sql);
                        tb_query.execute(&conn_pool).await.unwrap();
                    }
                }

                ExtractorConfig::PgCdc { url, .. } => {
                    let schema_sql = format!(r#"CREATE SCHEMA IF NOT EXISTS "{}""#, db_tb[0]);
                    let tb_sql = format!(
                        r#"CREATE TABLE IF NOT EXISTS "{}"."{}"(
                        slot_name character varying(64) not null,
                        update_timestamp timestamp without time zone default (now() at time zone 'utc'),
                        received_lsn character varying(64),
                        received_timestamp character varying(64),
                        flushed_lsn character varying(64),
                        flushed_timestamp character varying(64),
                        primary key(slot_name)
                    )"#,
                        db_tb[0], db_tb[1]
                    );

                    if !TaskUtil::check_tb_exist(url, &db_tb[0], &db_tb[1], &DbType::Pg).await {
                        let conn_pool = TaskUtil::create_pg_conn_pool(url, 1, true).await.unwrap();
                        let schema_query = sqlx::query(&schema_sql);
                        schema_query.execute(&conn_pool).await.unwrap();
                        let tb_query = sqlx::query(&tb_sql);
                        tb_query.execute(&conn_pool).await.unwrap();
                    }
                }

                _ => {}
            }
        }

        // create data marker table
        if let Some(data_marker) = sinker_data_marker {
            match &self.config.sinker {
                SinkerConfig::Mysql { url, .. } => {
                    let db_sql =
                        format!("CREATE DATABASE IF NOT EXISTS `{}`", data_marker.marker_db);
                    let tb_sql = format!(
                        "CREATE TABLE IF NOT EXISTS `{}`.`{}` (
                            data_origin_node varchar(255) NOT NULL,
                            src_node varchar(255) NOT NULL,
                            dst_node varchar(255) NOT NULL,
                            n bigint DEFAULT NULL,
                            PRIMARY KEY (data_origin_node, src_node, dst_node)
                        )",
                        data_marker.marker_db, data_marker.marker_tb
                    );

                    if !TaskUtil::check_tb_exist(
                        url,
                        &data_marker.marker_db,
                        &data_marker.marker_tb,
                        &DbType::Mysql,
                    )
                    .await
                    {
                        let conn_pool = TaskUtil::create_mysql_conn_pool(url, 1, true)
                            .await
                            .unwrap();
                        let db_query = sqlx::query(&db_sql);
                        db_query.execute(&conn_pool).await.unwrap();
                        let tb_query = sqlx::query(&tb_sql);
                        tb_query.execute(&conn_pool).await.unwrap();
                    }
                }

                SinkerConfig::Pg { url, .. } => {
                    let schema_sql =
                        format!(r#"CREATE SCHEMA IF NOT EXISTS "{}""#, data_marker.marker_db);
                    let tb_sql = format!(
                        r#"CREATE TABLE IF NOT EXISTS "{}"."{}" (
                            data_origin_node varchar(255) NOT NULL,
                            src_node varchar(255) NOT NULL,
                            dst_node varchar(255) NOT NULL,
                            n bigint DEFAULT NULL,
                            PRIMARY KEY (data_origin_node, src_node, dst_node)
                        )"#,
                        data_marker.marker_db, data_marker.marker_tb
                    );

                    if !TaskUtil::check_tb_exist(
                        url,
                        &data_marker.marker_db,
                        &data_marker.marker_tb,
                        &DbType::Pg,
                    )
                    .await
                    {
                        let conn_pool = TaskUtil::create_pg_conn_pool(url, 1, true).await.unwrap();
                        let schema_query = sqlx::query(&schema_sql);
                        schema_query.execute(&conn_pool).await.unwrap();
                        let tb_query = sqlx::query(&tb_sql);
                        tb_query.execute(&conn_pool).await.unwrap();
                    }
                }

                _ => {}
            }
        }
        Ok(())
    }
}
