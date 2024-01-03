use std::{
    fs::{self, File},
    io::Read,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use concurrent_queue::ConcurrentQueue;
use dt_common::{
    config::{
        config_enums::DbType, datamarker_config::DataMarkerSettingEnum,
        extractor_config::ExtractorConfig, sinker_config::SinkerConfig, task_config::TaskConfig,
    },
    error::Error,
    monitor::monitor::Monitor,
    utils::{rdb_filter::RdbFilter, time_util::TimeUtil},
};
use dt_connector::{
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
const LOG_LEVEL_PLACEHODLER: &str = "LOG_LEVEL_PLACEHODLER";
const LOG_DIR_PLACEHODLER: &str = "LOG_DIR_PLACEHODLER";
const DEFAULT_CHECK_LOG_DIR_PLACEHODLER: &str = "LOG_DIR_PLACEHODLER/check";

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
        let dbs = ExtractorUtil::list_dbs(url, &db_type).await?;
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
                    db: db.clone(),
                }),

                _ => None,
            };

            if let Some(extractor_config) = db_extractor_config {
                self.start_single_task(&extractor_config).await?;
                continue;
            }

            // start a task for each tb
            let tbs = ExtractorUtil::list_tbs(url, db, &db_type).await?;
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
                        db: db.clone(),
                        tb: tb.clone(),
                        sample_interval: *sample_interval,
                    },

                    ExtractorConfig::MongoSnapshot { url, .. } => ExtractorConfig::MongoSnapshot {
                        url: url.clone(),
                        db: db.clone(),
                        tb: tb.clone(),
                    },

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
            checkpoint_position: Position::None,
        }));

        let monitor_time_window_secs = self.config.pipeline.checkpoint_interval_secs as usize;
        let monitor_count_window = self.config.pipeline.buffer_size;

        // extractor
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
            )
            .await?;

        // sinkers
        let sinker_monitor = Arc::new(Mutex::new(Monitor::new(
            "sinker",
            monitor_time_window_secs,
            monitor_count_window,
        )));
        let transaction_command = self.fetch_transaction_command();
        let sinkers =
            SinkerUtil::create_sinkers(&self.config, transaction_command, sinker_monitor.clone())
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
            )
            .await?;

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
        let _ = try_join!(f1, f2, f3);
        Ok(())
    }

    async fn create_pipeline(
        &self,
        buffer: Arc<ConcurrentQueue<DtItem>>,
        shut_down: Arc<AtomicBool>,
        syncer: Arc<Mutex<Syncer>>,
        sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
        monitor: Arc<Mutex<Monitor>>,
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
            } => {
                let filter = RdbFilter::from_config_with_transaction(
                    &self.config.filter,
                    DbType::Mysql,
                    &self.config.datamarker,
                )?;

                let datamarker_filter = ExtractorUtil::datamarker_filter_builder(
                    &self.config.extractor,
                    &self.config.datamarker,
                )?;

                let extractor = ExtractorUtil::create_mysql_cdc_extractor(
                    base_extractor,
                    url,
                    binlog_filename,
                    *binlog_position,
                    *server_id,
                    filter,
                    &self.config.runtime.log_level,
                    datamarker_filter,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::PgSnapshot {
                url,
                db,
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
                heartbeat_interval_secs,
                ddl_command_table,
            } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Pg)?;
                let extractor = ExtractorUtil::create_pg_cdc_extractor(
                    base_extractor,
                    url,
                    slot_name,
                    pub_name,
                    start_lsn,
                    *heartbeat_interval_secs,
                    filter,
                    &self.config.runtime.log_level,
                    &ddl_command_table,
                    syncer,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::MongoSnapshot { url, db, tb } => {
                let extractor = ExtractorUtil::create_mongo_snapshot_extractor(
                    base_extractor,
                    url,
                    db,
                    tb,
                    resumer.clone(),
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::MongoCdc {
                url,
                resume_token,
                start_timestamp,
                source,
            } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Mongo)?;
                let extractor = ExtractorUtil::create_mongo_cdc_extractor(
                    base_extractor,
                    url,
                    resume_token,
                    start_timestamp,
                    source,
                    filter,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::MongoCheck {
                url,
                check_log_dir,
                batch_size,
            } => {
                let extractor = ExtractorUtil::create_mongo_check_extractor(
                    base_extractor,
                    url,
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

            ExtractorConfig::PgStruct { url, db } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Pg)?;
                let extractor = ExtractorUtil::create_pg_struct_extractor(
                    base_extractor,
                    url,
                    db,
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

            ExtractorConfig::RedisCdc {
                url,
                run_id,
                repl_offset,
                now_db_id,
                repl_port,
                heartbeat_interval_secs,
                heartbeat_key,
            } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Redis)?;
                let extractor = ExtractorUtil::create_redis_cdc_extractor(
                    base_extractor,
                    url,
                    run_id,
                    *repl_offset,
                    *repl_port,
                    *now_db_id,
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

    fn fetch_transaction_command(&self) -> String {
        match &self.config.datamarker.setting {
            Some(s) => match s {
                DataMarkerSettingEnum::Transaction {
                    transaction_command,
                    ..
                } => transaction_command.to_owned(),
            },
            None => String::from(""),
        }
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
                if let Some(dir) = check_log_dir {
                    if !dir.is_empty() {
                        config_str = config_str.replace(CHECK_LOG_DIR_PLACEHODLER, dir);
                    }
                }
            }
            _ => {}
        }

        config_str = config_str
            .replace(CHECK_LOG_DIR_PLACEHODLER, DEFAULT_CHECK_LOG_DIR_PLACEHODLER)
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
}
