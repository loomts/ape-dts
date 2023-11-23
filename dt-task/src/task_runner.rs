use std::{
    collections::HashMap,
    fs::{self, File},
    io::Read,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use async_rwlock::RwLock;
use concurrent_queue::ConcurrentQueue;
use dt_common::{
    config::{
        config_enums::DbType, datamarker_config::DataMarkerSettingEnum,
        extractor_config::ExtractorConfig, sinker_config::SinkerConfig, task_config::TaskConfig,
    },
    error::Error,
    log_monitor,
    monitor::monitor::{CounterType, Monitor},
    utils::{rdb_filter::RdbFilter, time_util::TimeUtil},
};
use dt_connector::{extractor::snapshot_resumer::SnapshotResumer, Extractor, Sinker};
use dt_meta::{dt_data::DtItem, position::Position, row_type::RowType, syncer::Syncer};
use dt_pipeline::{base_pipeline::BasePipeline, Pipeline};

use log4rs::config::RawConfig;
use tokio::try_join;

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
                    ExtractorConfig::MysqlSnapshot { url, .. } => ExtractorConfig::MysqlSnapshot {
                        url: url.clone(),
                        db: db.clone(),
                        tb: tb.clone(),
                    },

                    ExtractorConfig::PgSnapshot { url, .. } => ExtractorConfig::PgSnapshot {
                        url: url.clone(),
                        db: db.clone(),
                        tb: tb.clone(),
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

        // extractor
        let mut extractor = self
            .create_extractor(
                extractor_config,
                buffer.clone(),
                shut_down.clone(),
                syncer.clone(),
            )
            .await?;
        let extractor_monitor = extractor.get_monitor();

        // sinkers
        let transaction_command = self.fetch_transaction_command();
        let sinkers = SinkerUtil::create_sinkers(&self.config, transaction_command).await?;

        let mut sinker_monitors = Vec::new();
        for sinker in sinkers.iter() {
            sinker_monitors.push(sinker.lock().await.get_monitor())
        }

        // pipeline
        let mut pipeline = self
            .create_pipeline(buffer, shut_down.clone(), syncer, sinkers)
            .await?;
        let pipeline_monitor = pipeline.get_monitor();

        // start threads
        let f1 = tokio::spawn(async move {
            extractor.extract().await.unwrap();
            extractor.close().await.unwrap();
        });

        let f2 = tokio::spawn(async move {
            pipeline.start().await.unwrap();
            pipeline.stop().await.unwrap();
        });

        let interval_secs = self.config.pipeline.checkpoint_interval_secs as usize;
        let f3 = tokio::spawn(async move {
            Self::flush_monitors(
                interval_secs,
                shut_down,
                extractor_monitor,
                pipeline_monitor,
                sinker_monitors,
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
    ) -> Result<Box<dyn Pipeline + Send>, Error> {
        let parallelizer = ParallelizerUtil::create_parallelizer(&self.config).await?;
        let pipeline = BasePipeline {
            buffer,
            parallelizer,
            sinker_basic_config: self.config.sinker_basic.clone(),
            sinkers,
            shut_down,
            checkpoint_interval_secs: self.config.pipeline.checkpoint_interval_secs,
            batch_sink_interval_secs: self.config.pipeline.batch_sink_interval_secs,
            syncer,
            monitor: Arc::new(RwLock::new(Monitor::new_default())),
        };

        Ok(Box::new(pipeline))
    }

    async fn create_extractor(
        &self,
        extractor_config: &ExtractorConfig,
        buffer: Arc<ConcurrentQueue<DtItem>>,
        shut_down: Arc<AtomicBool>,
        syncer: Arc<Mutex<Syncer>>,
    ) -> Result<Box<dyn Extractor + Send>, Error> {
        let resumer =
            SnapshotResumer::new(&self.config.extractor_basic.db_type, &self.config.resumer)?;

        let extractor: Box<dyn Extractor + Send> = match extractor_config {
            ExtractorConfig::MysqlSnapshot { url, db, tb } => {
                let extractor = ExtractorUtil::create_mysql_snapshot_extractor(
                    url,
                    db,
                    tb,
                    self.config.pipeline.buffer_size,
                    resumer.clone(),
                    buffer,
                    &self.config.runtime.log_level,
                    shut_down,
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
                    url,
                    check_log_dir,
                    *batch_size,
                    buffer,
                    &self.config.runtime.log_level,
                    shut_down,
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
                    url,
                    binlog_filename,
                    *binlog_position,
                    *server_id,
                    buffer,
                    filter,
                    &self.config.runtime.log_level,
                    shut_down,
                    datamarker_filter,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::PgSnapshot { url, db, tb } => {
                let extractor = ExtractorUtil::create_pg_snapshot_extractor(
                    url,
                    db,
                    tb,
                    self.config.pipeline.buffer_size,
                    resumer.clone(),
                    buffer,
                    &self.config.runtime.log_level,
                    shut_down,
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
                    url,
                    check_log_dir,
                    *batch_size,
                    buffer,
                    &self.config.runtime.log_level,
                    shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::PgCdc {
                url,
                slot_name,
                start_lsn,
                heartbeat_interval_secs,
            } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Pg)?;
                let extractor = ExtractorUtil::create_pg_cdc_extractor(
                    url,
                    slot_name,
                    start_lsn,
                    *heartbeat_interval_secs,
                    buffer,
                    filter,
                    &self.config.runtime.log_level,
                    shut_down,
                    syncer,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::MongoSnapshot { url, db, tb } => {
                let extractor = ExtractorUtil::create_mongo_snapshot_extractor(
                    url,
                    db,
                    tb,
                    resumer.clone(),
                    buffer,
                    shut_down,
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
                    url,
                    resume_token,
                    start_timestamp,
                    source,
                    buffer,
                    filter,
                    shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::MysqlStruct { url, db } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Mysql)?;
                let extractor = ExtractorUtil::create_mysql_struct_extractor(
                    url,
                    db,
                    buffer,
                    filter,
                    &self.config.runtime.log_level,
                    shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::PgStruct { url, db } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Pg)?;
                let extractor = ExtractorUtil::create_pg_struct_extractor(
                    url,
                    db,
                    buffer,
                    filter,
                    &self.config.runtime.log_level,
                    shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::RedisSnapshot { url, repl_port } => {
                let filter = RdbFilter::from_config(&self.config.filter, DbType::Redis)?;
                let extractor = ExtractorUtil::create_redis_snapshot_extractor(
                    url, *repl_port, buffer, filter, shut_down,
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
                    url,
                    run_id,
                    *repl_offset,
                    *repl_port,
                    *now_db_id,
                    *heartbeat_interval_secs,
                    heartbeat_key,
                    buffer,
                    filter,
                    shut_down,
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
                let extractor = ExtractorUtil::create_kafka_extractor(
                    url,
                    group,
                    topic,
                    *partition,
                    *offset,
                    *ack_interval_secs,
                    &self.config.sinker_basic,
                    buffer,
                    shut_down,
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
        interval_secs: usize,
        shut_down: Arc<AtomicBool>,
        extractor_monitor: Option<Arc<RwLock<Monitor>>>,
        pipeline_monitor: Option<Arc<RwLock<Monitor>>>,
        mut sinker_monitors: Vec<Option<Arc<RwLock<Monitor>>>>,
    ) {
        // override the interval_secs
        if let Some(monitor) = &extractor_monitor {
            monitor.write().await.time_window_secs = interval_secs;
        }
        if let Some(monitor) = &pipeline_monitor {
            monitor.write().await.time_window_secs = interval_secs;
        }
        for monitor in sinker_monitors.iter_mut() {
            if let Some(monitor) = monitor {
                monitor.write().await.time_window_secs = interval_secs;
            }
        }

        loop {
            // do an extra flush before exit if task finished
            let finished = shut_down.load(Ordering::Acquire);
            if !finished {
                TimeUtil::sleep_millis(interval_secs as u64 * 1000).await;
            }

            // aggregate extractor counters
            if let Some(monitor) = &extractor_monitor {
                for (counter_type, counter) in monitor.write().await.time_window_counters.iter_mut()
                {
                    counter.refresh_window();
                    let agrregate = match counter_type {
                        _ => 0,
                    };
                    log_monitor!("extractor | {} | {}", counter_type.to_string(), agrregate)
                }

                for (counter_type, counter) in monitor.read().await.accumulate_counters.iter() {
                    log_monitor!(
                        "extractor | {} | {}",
                        counter_type.to_string(),
                        counter.value
                    )
                }
            }

            // aggregate pipeline counters
            if let Some(monitor) = &pipeline_monitor {
                for (counter_type, counter) in monitor.write().await.time_window_counters.iter_mut()
                {
                    counter.refresh_window();
                    let agrregate = match counter_type {
                        CounterType::BufferSize => counter.avg_by_count(),
                        _ => 0,
                    };
                    log_monitor!("pipeline | {} | {}", counter_type.to_string(), agrregate)
                }

                for (counter_type, counter) in monitor.read().await.accumulate_counters.iter() {
                    log_monitor!(
                        "pipeline | {} | {}",
                        counter_type.to_string(),
                        counter.value
                    )
                }
            }

            // aggregate sinker monitors
            let mut time_window_aggregates = HashMap::new();
            let mut accumulate_aggregates = HashMap::new();
            for monitor in sinker_monitors.iter_mut() {
                if monitor.is_none() {
                    continue;
                }

                if let Some(monitor) = monitor {
                    // time window counters
                    for (counter_type, counter) in
                        monitor.write().await.time_window_counters.iter_mut()
                    {
                        counter.refresh_window();
                        let (sum, count) =
                            if let Some((sum, count)) = time_window_aggregates.get(counter_type) {
                                (*sum, *count)
                            } else {
                                (0, 0)
                            };
                        time_window_aggregates.insert(
                            counter_type.clone(),
                            (counter.sum() + sum, counter.count() + count),
                        );
                    }

                    // accumulate counters
                    for (counter_type, counter) in monitor.read().await.accumulate_counters.iter() {
                        let sum = if let Some(sum) = accumulate_aggregates.get(counter_type) {
                            *sum
                        } else {
                            0
                        };
                        accumulate_aggregates.insert(counter_type.clone(), counter.value + sum);
                    }
                }
            }

            for (counter_type, (sum, count)) in time_window_aggregates {
                let agrregate = match counter_type {
                    CounterType::BatchWriteFailures | CounterType::SerialWrites => sum,

                    CounterType::Records => sum / interval_secs,

                    CounterType::BytesPerQuery
                    | CounterType::RecordsPerQuery
                    | CounterType::RtPerQuery => {
                        if count > 0 {
                            sum / count
                        } else {
                            0
                        }
                    }
                    _ => 0,
                };
                log_monitor!("sinker | {} | {}", counter_type.to_string(), agrregate);
            }

            for (counter_type, sum) in accumulate_aggregates {
                log_monitor!("sinker | {} | {}", counter_type.to_string(), sum);
            }

            if finished {
                break;
            }
        }
    }
}
