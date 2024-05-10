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
use dt_common::meta::{dt_data::DtItem, position::Position, row_type::RowType, syncer::Syncer};
use dt_common::{
    config::{
        config_enums::DbType, config_token_parser::ConfigTokenParser,
        extractor_config::ExtractorConfig, sinker_config::SinkerConfig, task_config::TaskConfig,
    },
    error::Error,
    log_info,
    monitor::monitor::Monitor,
    rdb_filter::RdbFilter,
    utils::{sql_util::SqlUtil, time_util::TimeUtil},
};
use dt_connector::{
    data_marker::DataMarker,
    extractor::resumer::{cdc_resumer::CdcResumer, snapshot_resumer::SnapshotResumer},
    rdb_router::RdbRouter,
    Sinker,
};
use dt_pipeline::{base_pipeline::BasePipeline, lua_processor::LuaProcessor, Pipeline};

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

        let db_type = &self.config.extractor_basic.db_type;
        let router = RdbRouter::from_config(&self.config.router, db_type)?;
        let snapshot_resumer = SnapshotResumer::from_config(&self.config.resumer, db_type)?;
        let cdc_resumer = CdcResumer::from_config(&self.config.resumer)?;

        match &self.config.extractor {
            ExtractorConfig::MysqlStruct { url, .. }
            | ExtractorConfig::PgStruct { url, .. }
            | ExtractorConfig::MysqlSnapshot { url, .. }
            | ExtractorConfig::PgSnapshot { url, .. }
            | ExtractorConfig::MongoSnapshot { url, .. } => {
                self.start_multi_task(url, &router, &snapshot_resumer, &cdc_resumer)
                    .await?
            }

            _ => {
                self.start_single_task(
                    &self.config.extractor,
                    &router,
                    &snapshot_resumer,
                    &cdc_resumer,
                )
                .await?
            }
        };

        Ok(())
    }

    async fn start_multi_task(
        &self,
        url: &str,
        router: &RdbRouter,
        snapshot_resumer: &SnapshotResumer,
        cdc_resumer: &CdcResumer,
    ) -> Result<(), Error> {
        let db_type = &self.config.extractor_basic.db_type;
        let mut filter = RdbFilter::from_config(&self.config.filter, db_type)?;

        let dbs = TaskUtil::list_dbs(url, db_type).await?;
        for db in dbs.iter() {
            if filter.filter_db(db) {
                log_info!("db: {} filtered", db);
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
                self.start_single_task(&extractor_config, router, snapshot_resumer, cdc_resumer)
                    .await?;
                continue;
            }

            // start a task for each tb
            let tbs = TaskUtil::list_tbs(url, db, db_type).await?;
            for tb in tbs.iter() {
                if snapshot_resumer.check_finished(db, tb) {
                    log_info!("db: {}, tb: {} already finished", db, tb);
                    continue;
                }

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

                self.start_single_task(&tb_extractor_config, router, snapshot_resumer, cdc_resumer)
                    .await?;
            }
        }
        Ok(())
    }

    async fn start_single_task(
        &self,
        extractor_config: &ExtractorConfig,
        router: &RdbRouter,
        snapshot_resumer: &SnapshotResumer,
        cdc_resumer: &CdcResumer,
    ) -> Result<(), Error> {
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
        let mut extractor = ExtractorUtil::create_extractor(
            &self.config,
            extractor_config,
            buffer.clone(),
            shut_down.clone(),
            syncer.clone(),
            extractor_monitor.clone(),
            extractor_data_marker,
            router.clone(),
            snapshot_resumer.clone(),
            cdc_resumer.clone(),
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

        let lua_processor = if let Some(processor_config) = &self.config.processor {
            Some(LuaProcessor {
                lua_code: processor_config.lua_code.clone(),
            })
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
            lua_processor,
        };

        Ok(Box::new(pipeline))
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

                    TaskUtil::check_and_create_tb(
                        url,
                        &db_tb[0],
                        &db_tb[1],
                        &db_sql,
                        &tb_sql,
                        &DbType::Mysql,
                    )
                    .await?
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

                    TaskUtil::check_and_create_tb(
                        url,
                        &db_tb[0],
                        &db_tb[1],
                        &schema_sql,
                        &tb_sql,
                        &DbType::Pg,
                    )
                    .await?
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

                    TaskUtil::check_and_create_tb(
                        url,
                        &data_marker.marker_db,
                        &data_marker.marker_tb,
                        &db_sql,
                        &tb_sql,
                        &DbType::Mysql,
                    )
                    .await?
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

                    TaskUtil::check_and_create_tb(
                        url,
                        &data_marker.marker_db,
                        &data_marker.marker_tb,
                        &schema_sql,
                        &tb_sql,
                        &DbType::Pg,
                    )
                    .await?
                }

                _ => {}
            }
        }
        Ok(())
    }
}
