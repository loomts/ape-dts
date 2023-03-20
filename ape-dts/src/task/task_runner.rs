use std::{
    fs::File,
    io::Read,
    sync::{atomic::AtomicBool, Arc, Mutex},
};

use concurrent_queue::ConcurrentQueue;
use futures::future::join;
use log4rs::config::RawConfig;

use crate::{
    config::{
        extractor_config::ExtractorConfig, pipeline_config::PipelineType,
        sinker_config::SinkerConfig, task_config::TaskConfig,
    },
    error::Error,
    extractor::rdb_filter::RdbFilter,
    meta::row_data::RowData,
    metric::Metric,
    pipeline::{
        default_pipeline::DefaultPipeline, merge_pipeline::MergePipeline,
        snapshot_pipeline::SnapshotPipeline,
    },
    sinker::rdb_router::RdbRouter,
    traits::{Extractor, Pipeline},
};

use super::{extractor_util::ExtractorUtil, sinker_util::SinkerUtil};

pub struct TaskRunner {
    config: TaskConfig,
}

const LOG_LEVEL_PLACEHODLER: &str = "LOG_LEVEL_PLACEHODLER";
const LOG_DIR_PLACEHODLER: &str = "LOG_DIR_PLACEHODLER";
const LOG4RS_YAML: &str = "log4rs.yaml";

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
            ExtractorConfig::MysqlSnapshot { .. } | ExtractorConfig::PgSnapshot { .. } => {
                self.start_multi_task().await?
            }

            _ => self.start_single_task(&self.config.extractor).await?,
        };

        Ok(())
    }

    async fn start_multi_task(&self) -> Result<(), Error> {
        let filter = RdbFilter::from_config(&self.config.filter)?;
        for do_tb in filter.do_tbs.iter() {
            let single_extractor_config = match &self.config.extractor {
                ExtractorConfig::MysqlSnapshot { url, .. } => ExtractorConfig::MysqlSnapshot {
                    url: url.clone(),
                    do_tb: do_tb.clone(),
                },

                ExtractorConfig::PgSnapshot { url, .. } => ExtractorConfig::PgSnapshot {
                    url: url.clone(),
                    do_tb: do_tb.clone(),
                },

                _ => {
                    return Err(Error::Unexpected {
                        error: "unexpected extractor config type for rdb snapshot task".to_string(),
                    });
                }
            };

            self.start_single_task(&single_extractor_config).await?;
        }

        Ok(())
    }

    async fn start_single_task(&self, extractor_config: &ExtractorConfig) -> Result<(), Error> {
        let buffer = ConcurrentQueue::bounded(self.config.pipeline.buffer_size);
        let shut_down = AtomicBool::new(false);
        let metric = Arc::new(Mutex::new(Metric {
            position: "".to_string(),
        }));

        let mut extractor = self
            .create_extractor(&extractor_config, &buffer, &shut_down, metric.clone())
            .await?;
        let mut pipeline = self.create_pipeline(&buffer, &shut_down, metric).await?;

        let result = join(extractor.extract(), pipeline.start()).await;
        pipeline.stop().await?;
        extractor.close().await?;
        if result.0.is_err() {
            return result.0;
        }
        result.1
    }

    async fn create_extractor<'a>(
        &self,
        extractor_config: &ExtractorConfig,
        buffer: &'a ConcurrentQueue<RowData>,
        shut_down: &'a AtomicBool,
        metric: Arc<Mutex<Metric>>,
    ) -> Result<Box<dyn Extractor + 'a + Send>, Error> {
        let extractor: Box<dyn Extractor + Send> = match extractor_config {
            ExtractorConfig::MysqlSnapshot { url, do_tb } => {
                let extractor = ExtractorUtil::create_mysql_snapshot_extractor(
                    &url,
                    &do_tb,
                    self.config.pipeline.buffer_size,
                    &buffer,
                    &self.config.runtime.log_level,
                    &shut_down,
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
                let filter = RdbFilter::from_config(&self.config.filter)?;
                let extractor = ExtractorUtil::create_mysql_cdc_extractor(
                    &url,
                    &binlog_filename,
                    *binlog_position,
                    *server_id,
                    &buffer,
                    filter,
                    &self.config.runtime.log_level,
                    &shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::PgSnapshot { url, do_tb } => {
                let extractor = ExtractorUtil::create_pg_snapshot_extractor(
                    &url,
                    &do_tb,
                    self.config.pipeline.buffer_size,
                    &buffer,
                    &self.config.runtime.log_level,
                    &shut_down,
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
                let filter = RdbFilter::from_config(&self.config.filter)?;
                let extractor = ExtractorUtil::create_pg_cdc_extractor(
                    &url,
                    &slot_name,
                    &start_lsn,
                    *heartbeat_interval_secs,
                    &buffer,
                    filter,
                    &self.config.runtime.log_level,
                    &shut_down,
                    metric,
                )
                .await?;
                Box::new(extractor)
            }
        };
        Ok(extractor)
    }

    async fn create_pipeline<'a>(
        &self,
        buffer: &'a ConcurrentQueue<RowData>,
        shut_down: &'a AtomicBool,
        metric: Arc<Mutex<Metric>>,
    ) -> Result<Box<dyn Pipeline + 'a + Send>, Error> {
        let router = RdbRouter::from_config(&self.config.router)?;
        let sub_sinkers = match &self.config.sinker {
            SinkerConfig::Mysql { url, batch_size } => {
                SinkerUtil::create_mysql_sinker(
                    &url,
                    &router,
                    &self.config.runtime.log_level,
                    self.config.pipeline.parallel_size,
                    *batch_size,
                )
                .await?
            }

            SinkerConfig::Pg { url, batch_size } => {
                SinkerUtil::create_pg_sinker(
                    &url,
                    &router,
                    &self.config.runtime.log_level,
                    self.config.pipeline.parallel_size,
                    *batch_size,
                )
                .await?
            }
        };

        let pipeline: Box<dyn Pipeline + 'a + Send> = match self.config.pipeline.pipeline_type {
            PipelineType::MERGE => {
                let merger = SinkerUtil::create_rdb_merger(
                    &self.config.runtime.log_level,
                    &self.config.sinker,
                )
                .await?;
                Box::new(MergePipeline {
                    buffer,
                    merger,
                    sinkers: sub_sinkers,
                    shut_down,
                    metric,
                    checkpoint_interval_secs: self.config.pipeline.checkpoint_interval_secs,
                })
            }

            PipelineType::SNAPSHOT => Box::new(SnapshotPipeline {
                buffer,
                sinkers: sub_sinkers,
                shut_down,
                metric,
                checkpoint_interval_secs: self.config.pipeline.checkpoint_interval_secs,
            }),

            PipelineType::DEFAULT => {
                let partitioner = SinkerUtil::create_rdb_partitioner(
                    &self.config.runtime.log_level,
                    &self.config.sinker,
                )
                .await?;
                Box::new(DefaultPipeline {
                    buffer,
                    partitioner: Box::new(partitioner),
                    sinkers: sub_sinkers,
                    shut_down,
                    metric,
                    checkpoint_interval_secs: self.config.pipeline.checkpoint_interval_secs,
                })
            }
        };
        Ok(pipeline)
    }

    fn init_log4rs(&self) -> Result<(), Error> {
        let mut config_str = String::new();
        File::open(LOG4RS_YAML)?.read_to_string(&mut config_str)?;
        config_str = config_str
            .replace(LOG_DIR_PLACEHODLER, &self.config.runtime.log_dir)
            .replace(LOG_LEVEL_PLACEHODLER, &self.config.runtime.log_level);

        let config: RawConfig = serde_yaml::from_str(&config_str)?;
        log4rs::init_raw_config(config).unwrap();
        Ok(())
    }
}
