use std::{
    collections::VecDeque,
    fs::{self, File},
    io::Read,
    panic,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    time::Duration,
};

use anyhow::{bail, Context};
use dt_common::{
    config::{
        config_enums::{DbType, PipelineType},
        config_token_parser::ConfigTokenParser,
        extractor_config::ExtractorConfig,
        sinker_config::SinkerConfig,
        task_config::TaskConfig,
    },
    error::Error,
    log_finished, log_info,
    meta::{avro::avro_converter::AvroConverter, dt_queue::DtQueue},
    monitor::{group_monitor::GroupMonitor, monitor::Monitor, FlushableMonitor},
    rdb_filter::RdbFilter,
    utils::{sql_util::SqlUtil, time_util::TimeUtil},
};
use dt_common::{
    log_error,
    meta::{position::Position, row_type::RowType, syncer::Syncer},
};
use dt_connector::{
    data_marker::DataMarker,
    extractor::resumer::{cdc_resumer::CdcResumer, snapshot_resumer::SnapshotResumer},
    rdb_router::RdbRouter,
    Sinker,
};
use dt_pipeline::{
    base_pipeline::BasePipeline, http_server_pipeline::HttpServerPipeline,
    lua_processor::LuaProcessor, Pipeline,
};

use log4rs::config::RawConfig;
use ratelimit::Ratelimiter;
use tokio::{task::JoinSet, try_join};

use crate::task_util::TaskUtil;

use super::{
    extractor_util::ExtractorUtil, parallelizer_util::ParallelizerUtil, sinker_util::SinkerUtil,
};

#[derive(Clone)]
pub struct TaskRunner {
    config: TaskConfig,
    extractor_monitor: Arc<Mutex<GroupMonitor>>,
    pipeline_monitor: Arc<Mutex<GroupMonitor>>,
    sinker_monitor: Arc<Mutex<GroupMonitor>>,
}

const CHECK_LOG_DIR_PLACEHODLER: &str = "CHECK_LOG_DIR_PLACEHODLER";
const STATISTIC_LOG_DIR_PLACEHODLER: &str = "STATISTIC_LOG_DIR_PLACEHODLER";
const LOG_LEVEL_PLACEHODLER: &str = "LOG_LEVEL_PLACEHODLER";
const LOG_DIR_PLACEHODLER: &str = "LOG_DIR_PLACEHODLER";
const DEFAULT_CHECK_LOG_DIR_PLACEHODLER: &str = "LOG_DIR_PLACEHODLER/check";
const DEFAULT_STATISTIC_LOG_DIR_PLACEHODLER: &str = "LOG_DIR_PLACEHODLER/statistic";

impl TaskRunner {
    pub fn new(task_config_file: &str) -> anyhow::Result<Self> {
        let config = TaskConfig::new(task_config_file)
            .with_context(|| format!("invalid configs in [{}]", task_config_file))?;
        Ok(Self {
            config,
            extractor_monitor: Arc::new(Mutex::new(GroupMonitor::new("extractor", "global"))),
            pipeline_monitor: Arc::new(Mutex::new(GroupMonitor::new("pipeline", "global"))),
            sinker_monitor: Arc::new(Mutex::new(GroupMonitor::new("sinker", "global"))),
        })
    }

    pub async fn start_task(&self, enable_log4rs: bool) -> anyhow::Result<()> {
        if enable_log4rs {
            self.init_log4rs()?;
        }

        panic::set_hook(Box::new(|panic_info| {
            let backtrace = std::backtrace::Backtrace::capture();
            log_error!("panic: {}\nbacktrace:\n{}", panic_info, backtrace);
        }));

        let db_type = &self.config.extractor_basic.db_type;
        let router = RdbRouter::from_config(&self.config.router, db_type)?;
        let snapshot_resumer = SnapshotResumer::from_config(&self.config.resumer)?;
        let cdc_resumer = CdcResumer::from_config(&self.config.resumer)?;

        match &self.config.extractor {
            ExtractorConfig::MysqlStruct { url, .. }
            | ExtractorConfig::PgStruct { url, .. }
            | ExtractorConfig::MysqlSnapshot { url, .. }
            | ExtractorConfig::PgSnapshot { url, .. }
            | ExtractorConfig::MongoSnapshot { url, .. }
            | ExtractorConfig::FoxlakeS3 { url, .. } => {
                self.start_multi_task(url, &router, &snapshot_resumer, &cdc_resumer)
                    .await?
            }

            _ => {
                self.clone()
                    .start_single_task(
                        &self.config.extractor,
                        &router,
                        &snapshot_resumer,
                        &cdc_resumer,
                    )
                    .await?
            }
        };

        log_finished!("task finished");
        Ok(())
    }

    async fn start_multi_task(
        &self,
        url: &str,
        router: &RdbRouter,
        snapshot_resumer: &SnapshotResumer,
        cdc_resumer: &CdcResumer,
    ) -> anyhow::Result<()> {
        let db_type = &self.config.extractor_basic.db_type;
        let mut filter = RdbFilter::from_config(&self.config.filter, db_type)?;

        let mut pending_tbs = VecDeque::new();
        let schemas = TaskUtil::list_schemas(url, db_type).await?;
        for schema in schemas.iter() {
            if filter.filter_schema(schema) {
                log_info!("schema: {} filtered", schema);
                continue;
            }

            // start a task for each schema
            let schema_extractor_config = match &self.config.extractor {
                ExtractorConfig::MysqlStruct { url, .. } => Some(ExtractorConfig::MysqlStruct {
                    url: url.clone(),
                    db: schema.clone(),
                }),

                ExtractorConfig::PgStruct { url, .. } => Some(ExtractorConfig::PgStruct {
                    url: url.clone(),
                    schema: schema.clone(),
                }),

                _ => None,
            };

            if let Some(extractor_config) = schema_extractor_config {
                self.clone()
                    .start_single_task(&extractor_config, router, snapshot_resumer, cdc_resumer)
                    .await?;
                continue;
            }

            // find pending tables
            let tbs = TaskUtil::list_tbs(url, schema, db_type).await?;
            for tb in tbs.iter() {
                if snapshot_resumer.check_finished(schema, tb) {
                    log_info!("schema: {}, tb: {}, already finished", schema, tb);
                    continue;
                }
                if filter.filter_event(schema, tb, &RowType::Insert) {
                    log_info!("schema: {}, tb: {}, insert events filtered", schema, tb);
                    continue;
                }
                pending_tbs.push_back((schema.to_owned(), tb.to_owned()));
            }
        }

        // start a thread to flush global monitors
        let global_shut_down = Arc::new(AtomicBool::new(false));
        let global_shut_down_clone = global_shut_down.clone();
        let interval_secs = self.config.pipeline.checkpoint_interval_secs;
        let extractor_monitor = self.extractor_monitor.clone();
        let pipeline_monitor = self.pipeline_monitor.clone();
        let sinker_monitor = self.sinker_monitor.clone();
        let global_monitor_task = tokio::spawn(async move {
            Self::flush_group_monitors(
                interval_secs,
                global_shut_down_clone,
                extractor_monitor,
                pipeline_monitor,
                sinker_monitor,
            )
            .await
        });

        // process all tables in parallel
        let tb_parallel_size = self.config.runtime.tb_parallel_size;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(tb_parallel_size));
        let mut join_set: JoinSet<(String, anyhow::Result<()>)> = JoinSet::new();

        // initialize the task pool to its maximum capacity
        while join_set.len() < tb_parallel_size && !pending_tbs.is_empty() {
            if let Some((schema, tb)) = pending_tbs.pop_front() {
                self.clone()
                    .spawn_single_task(
                        &schema,
                        &tb,
                        router,
                        snapshot_resumer,
                        cdc_resumer,
                        &mut join_set,
                        &semaphore,
                    )
                    .await?;
            }
        }

        // when a task is completed, if there are still pending tables, add a new task
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((_, Ok(()))) => {
                    if let Some((schema, tb)) = pending_tbs.pop_front() {
                        self.clone()
                            .spawn_single_task(
                                &schema,
                                &tb,
                                router,
                                snapshot_resumer,
                                cdc_resumer,
                                &mut join_set,
                                &semaphore,
                            )
                            .await?;
                    }
                }
                Ok((single_task_id, Err(e))) => {
                    log_error!("single task: [{}] failed, error: {}", single_task_id, e)
                }
                Err(e) => log_error!("join error: {}", e),
            }
        }

        global_shut_down.store(true, Ordering::Release);
        global_monitor_task.await?;
        Ok(())
    }

    async fn spawn_single_task(
        self,
        schema: &str,
        tb: &str,
        router: &RdbRouter,
        snapshot_resumer: &SnapshotResumer,
        cdc_resumer: &CdcResumer,
        join_set: &mut JoinSet<(String, anyhow::Result<()>)>,
        semaphore: &Arc<tokio::sync::Semaphore>,
    ) -> anyhow::Result<()> {
        let tb_extractor_config = match &self.config.extractor {
            ExtractorConfig::MysqlSnapshot {
                url,
                sample_interval,
                parallel_size,
                batch_size,
                ..
            } => ExtractorConfig::MysqlSnapshot {
                url: url.clone(),
                db: schema.into(),
                tb: tb.into(),
                sample_interval: *sample_interval,
                parallel_size: *parallel_size,
                batch_size: *batch_size,
            },

            ExtractorConfig::PgSnapshot {
                url,
                sample_interval,
                batch_size,
                ..
            } => ExtractorConfig::PgSnapshot {
                url: url.clone(),
                schema: schema.into(),
                tb: tb.into(),
                sample_interval: *sample_interval,
                batch_size: *batch_size,
            },

            ExtractorConfig::MongoSnapshot { url, app_name, .. } => {
                ExtractorConfig::MongoSnapshot {
                    url: url.clone(),
                    app_name: app_name.clone(),
                    db: schema.into(),
                    tb: tb.into(),
                }
            }

            ExtractorConfig::FoxlakeS3 {
                url,
                s3_config,
                batch_size,
                ..
            } => ExtractorConfig::FoxlakeS3 {
                url: url.into(),
                schema: schema.into(),
                tb: tb.into(),
                s3_config: s3_config.clone(),
                batch_size: *batch_size,
            },

            _ => {
                bail! {Error::ConfigError("unsupported extractor config".into())};
            }
        };

        let single_task_id = format!("{}.{}", schema, tb);
        let router = router.clone();
        let snapshot_resumer = snapshot_resumer.clone();
        let cdc_resumer = cdc_resumer.clone();
        let semaphore = Arc::clone(semaphore);
        let me = self.clone();
        join_set.spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let res = me
                .start_single_task(
                    &tb_extractor_config,
                    &router,
                    &snapshot_resumer,
                    &cdc_resumer,
                )
                .await;
            (single_task_id, res)
        });
        Ok(())
    }

    async fn start_single_task(
        self,
        extractor_config: &ExtractorConfig,
        router: &RdbRouter,
        snapshot_resumer: &SnapshotResumer,
        cdc_resumer: &CdcResumer,
    ) -> anyhow::Result<()> {
        let max_bytes = self.config.pipeline.buffer_memory_mb * 1024 * 1024;
        let buffer = Arc::new(DtQueue::new(
            self.config.pipeline.buffer_size,
            max_bytes as i64,
        ));

        let shut_down = Arc::new(AtomicBool::new(false));
        let syncer = Arc::new(Mutex::new(Syncer {
            received_position: Position::None,
            committed_position: Position::None,
        }));

        let (extractor_data_marker, sinker_data_marker) = if let Some(data_marker_config) =
            &self.config.data_marker
        {
            let extractor_data_marker =
                DataMarker::from_config(data_marker_config, &self.config.extractor_basic.db_type)?;
            let sinker_data_marker =
                DataMarker::from_config(data_marker_config, &self.config.sinker_basic.db_type)?;
            (Some(extractor_data_marker), Some(sinker_data_marker))
        } else {
            (None, None)
        };
        let rw_sinker_data_marker = sinker_data_marker
            .clone()
            .map(|data_marker| Arc::new(RwLock::new(data_marker)));

        let single_task_id = match extractor_config {
            ExtractorConfig::MysqlSnapshot { db, tb, .. } => format!("{}.{}", db, tb),
            ExtractorConfig::PgSnapshot { schema, tb, .. } => format!("{}.{}", schema, tb),
            ExtractorConfig::MongoSnapshot { db, tb, .. } => format!("{}.{}", db, tb),
            _ => String::new(),
        };

        // extractor
        let monitor_time_window_secs = self.config.pipeline.counter_time_window_secs as usize;
        let monitor_max_sub_count = self.config.pipeline.counter_max_sub_count as usize;
        let monitor_count_window = self.config.pipeline.buffer_size;
        let extractor_monitor = Arc::new(Mutex::new(Monitor::new(
            "extractor",
            &single_task_id,
            monitor_time_window_secs,
            monitor_max_sub_count,
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
            &single_task_id,
            monitor_time_window_secs,
            monitor_max_sub_count,
            monitor_count_window,
        )));
        let sinkers = SinkerUtil::create_sinkers(
            &self.config,
            extractor_config,
            sinker_monitor.clone(),
            rw_sinker_data_marker.clone(),
        )
        .await?;

        // pipeline
        let pipeline_monitor = Arc::new(Mutex::new(Monitor::new(
            "pipeline",
            &single_task_id,
            monitor_time_window_secs,
            monitor_max_sub_count,
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

        // add monitors to global monitors
        if let Ok(guard) = self.extractor_monitor.lock().as_mut() {
            guard.add_monitor(&single_task_id, extractor_monitor.clone());
        }
        if let Ok(guard) = self.pipeline_monitor.lock().as_mut() {
            guard.add_monitor(&single_task_id, pipeline_monitor.clone());
        }
        if let Ok(guard) = self.sinker_monitor.lock().as_mut() {
            guard.add_monitor(&single_task_id, sinker_monitor.clone());
        }

        // do pre operations before task starts
        self.pre_single_task(sinker_data_marker).await?;

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

        // finished log
        let (schema, tb) = match extractor_config {
            ExtractorConfig::MysqlSnapshot { db, tb, .. }
            | ExtractorConfig::MongoSnapshot { db, tb, .. } => (db.to_owned(), tb.to_owned()),
            ExtractorConfig::PgSnapshot { schema, tb, .. }
            | ExtractorConfig::FoxlakeS3 { schema, tb, .. } => (schema.to_owned(), tb.to_owned()),
            _ => (String::new(), String::new()),
        };
        if !tb.is_empty() {
            log_finished!(
                "{}",
                Position::RdbSnapshotFinished {
                    db_type: self.config.extractor_basic.db_type.to_string(),
                    schema,
                    tb,
                }
                .to_string()
            );
        }

        // remove monitors from global monitors
        if let Ok(guard) = self.extractor_monitor.lock().as_mut() {
            guard.remove_monitor(&single_task_id);
        }
        if let Ok(guard) = self.pipeline_monitor.lock().as_mut() {
            guard.remove_monitor(&single_task_id);
        }
        if let Ok(guard) = self.sinker_monitor.lock().as_mut() {
            guard.remove_monitor(&single_task_id);
        }

        Ok(())
    }

    async fn create_pipeline(
        &self,
        buffer: Arc<DtQueue>,
        shut_down: Arc<AtomicBool>,
        syncer: Arc<Mutex<Syncer>>,
        sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
        monitor: Arc<Mutex<Monitor>>,
        data_marker: Option<Arc<RwLock<DataMarker>>>,
    ) -> anyhow::Result<Box<dyn Pipeline + Send>> {
        match self.config.pipeline.pipeline_type {
            PipelineType::Basic => {
                let rps_limiter = if self.config.pipeline.max_rps > 0 {
                    Some(
                        Ratelimiter::builder(self.config.pipeline.max_rps, Duration::from_secs(1))
                            .max_tokens(self.config.pipeline.max_rps)
                            .initial_available(self.config.pipeline.max_rps)
                            .build()?,
                    )
                } else {
                    None
                };

                let lua_processor =
                    self.config
                        .processor
                        .as_ref()
                        .map(|processor_config| LuaProcessor {
                            lua_code: processor_config.lua_code.clone(),
                        });

                let parallelizer = ParallelizerUtil::create_parallelizer(
                    &self.config,
                    monitor.clone(),
                    rps_limiter,
                )
                .await?;

                let pipeline = BasePipeline {
                    buffer,
                    parallelizer,
                    sinker_config: self.config.sinker.clone(),
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

            PipelineType::HttpServer => {
                let meta_manager = ExtractorUtil::get_extractor_meta_manager(&self.config).await?;
                let avro_converter =
                    AvroConverter::new(meta_manager, self.config.pipeline.with_field_defs);
                let pipeline = HttpServerPipeline::new(
                    buffer,
                    syncer,
                    monitor,
                    avro_converter,
                    self.config.pipeline.checkpoint_interval_secs,
                    self.config.pipeline.batch_sink_interval_secs,
                    &self.config.pipeline.http_host,
                    self.config.pipeline.http_port,
                );
                Ok(Box::new(pipeline))
            }
        }
    }

    fn init_log4rs(&self) -> anyhow::Result<()> {
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
        log4rs::init_raw_config(config)?;
        Ok(())
    }

    async fn flush_monitors(
        interval_secs: u64,
        shut_down: Arc<AtomicBool>,
        extractor_monitor: Arc<Mutex<Monitor>>,
        pipeline_monitor: Arc<Mutex<Monitor>>,
        sinker_monitor: Arc<Mutex<Monitor>>,
    ) {
        Self::flush_monitors_generic(
            interval_secs,
            shut_down,
            extractor_monitor,
            pipeline_monitor,
            sinker_monitor,
        )
        .await
    }

    async fn flush_group_monitors(
        interval_secs: u64,
        shut_down: Arc<AtomicBool>,
        extractor_monitor: Arc<Mutex<GroupMonitor>>,
        pipeline_monitor: Arc<Mutex<GroupMonitor>>,
        sinker_monitor: Arc<Mutex<GroupMonitor>>,
    ) {
        Self::flush_monitors_generic(
            interval_secs,
            shut_down,
            extractor_monitor,
            pipeline_monitor,
            sinker_monitor,
        )
        .await
    }

    async fn flush_monitors_generic<T>(
        interval_secs: u64,
        shut_down: Arc<AtomicBool>,
        extractor_monitor: Arc<Mutex<T>>,
        pipeline_monitor: Arc<Mutex<T>>,
        sinker_monitor: Arc<Mutex<T>>,
    ) where
        T: FlushableMonitor,
    {
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

    async fn pre_single_task(&self, sinker_data_marker: Option<DataMarker>) -> anyhow::Result<()> {
        // create heartbeat table
        let schema_tb = match &self.config.extractor {
            ExtractorConfig::MysqlCdc { heartbeat_tb, .. }
            | ExtractorConfig::PgCdc { heartbeat_tb, .. } => ConfigTokenParser::parse(
                heartbeat_tb,
                &['.'],
                &SqlUtil::get_escape_pairs(&self.config.extractor_basic.db_type),
            ),
            _ => vec![],
        };

        if schema_tb.len() == 2 {
            match &self.config.extractor {
                ExtractorConfig::MysqlCdc { url, .. } => {
                    let db_sql = format!("CREATE DATABASE IF NOT EXISTS `{}`", schema_tb[0]);
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
                        schema_tb[0], schema_tb[1]
                    );

                    TaskUtil::check_and_create_tb(
                        url,
                        &schema_tb[0],
                        &schema_tb[1],
                        &db_sql,
                        &tb_sql,
                        &DbType::Mysql,
                    )
                    .await?
                }

                ExtractorConfig::PgCdc { url, .. } => {
                    let schema_sql = format!(r#"CREATE SCHEMA IF NOT EXISTS "{}""#, schema_tb[0]);
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
                        schema_tb[0], schema_tb[1]
                    );

                    TaskUtil::check_and_create_tb(
                        url,
                        &schema_tb[0],
                        &schema_tb[1],
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
                    let db_sql = format!(
                        "CREATE DATABASE IF NOT EXISTS `{}`",
                        data_marker.marker_schema
                    );
                    let tb_sql = format!(
                        "CREATE TABLE IF NOT EXISTS `{}`.`{}` (
                            data_origin_node varchar(255) NOT NULL,
                            src_node varchar(255) NOT NULL,
                            dst_node varchar(255) NOT NULL,
                            n bigint DEFAULT NULL,
                            PRIMARY KEY (data_origin_node, src_node, dst_node)
                        )",
                        data_marker.marker_schema, data_marker.marker_tb
                    );

                    TaskUtil::check_and_create_tb(
                        url,
                        &data_marker.marker_schema,
                        &data_marker.marker_tb,
                        &db_sql,
                        &tb_sql,
                        &DbType::Mysql,
                    )
                    .await?
                }

                SinkerConfig::Pg { url, .. } => {
                    let schema_sql = format!(
                        r#"CREATE SCHEMA IF NOT EXISTS "{}""#,
                        data_marker.marker_schema
                    );
                    let tb_sql = format!(
                        r#"CREATE TABLE IF NOT EXISTS "{}"."{}" (
                            data_origin_node varchar(255) NOT NULL,
                            src_node varchar(255) NOT NULL,
                            dst_node varchar(255) NOT NULL,
                            n bigint DEFAULT NULL,
                            PRIMARY KEY (data_origin_node, src_node, dst_node)
                        )"#,
                        data_marker.marker_schema, data_marker.marker_tb
                    );

                    TaskUtil::check_and_create_tb(
                        url,
                        &data_marker.marker_schema,
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
