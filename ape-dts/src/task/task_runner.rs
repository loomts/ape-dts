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
        config_loader::ConfigLoader, extractor_config::ExtractorConfig,
        filter_config::FilterConfig, router_config::RouterConfig, runtime_config::RuntimeConfig,
        sinker_config::SinkerConfig,
    },
    error::Error,
    extractor::rdb_filter::RdbFilter,
    meta::row_data::RowData,
    metric::Metric,
    sinker::{parallel_sinker::ParallelSinker, rdb_router::RdbRouter},
    traits::Extractor,
};

use super::task_util::TaskUtil;

pub struct TaskRunner {
    extractor_config: ExtractorConfig,
    sinker_config: SinkerConfig,
    runtime_config: RuntimeConfig,
    filter_config: FilterConfig,
    router_config: RouterConfig,
}

const LOG_LEVEL_PLACEHODLER: &str = "LOG_LEVEL_PLACEHODLER";
const LOG_DIR_PLACEHODLER: &str = "LOG_DIR_PLACEHODLER";
const LOG4RS_YAML: &str = "log4rs.yaml";

impl TaskRunner {
    pub async fn new(task_config: String) -> Self {
        let (extractor_config, sinker_config, runtime_config, filter_config, router_config) =
            ConfigLoader::load(&task_config).unwrap();
        Self {
            extractor_config,
            sinker_config,
            runtime_config,
            filter_config,
            router_config,
        }
    }

    pub async fn start_task(&self, enable_log4rs: bool) -> Result<(), Error> {
        if enable_log4rs {
            self.init_log4rs()?;
        }

        match &self.extractor_config {
            ExtractorConfig::MysqlSnapshot { .. } | ExtractorConfig::PgSnapshot { .. } => {
                self.start_multi_task().await?
            }

            _ => self.start_single_task(&self.extractor_config).await?,
        };

        Ok(())
    }

    async fn start_multi_task(&self) -> Result<(), Error> {
        let filter = RdbFilter::from_config(&self.filter_config)?;
        for do_tb in filter.do_tbs.iter() {
            let single_extractor_config = match &self.extractor_config {
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
        let buffer = ConcurrentQueue::bounded(self.runtime_config.buffer_size);
        let shut_down = AtomicBool::new(false);
        let metric = Arc::new(Mutex::new(Metric {
            position: "".to_string(),
        }));

        let mut extractor = self
            .create_extractor(&extractor_config, &buffer, &shut_down, metric.clone())
            .await?;

        let mut sinker = self.create_sinker(&buffer, &shut_down, metric).await?;

        let result = join(extractor.extract(), sinker.sink()).await;
        sinker.close().await?;
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
                let extractor = TaskUtil::create_mysql_snapshot_extractor(
                    &url,
                    &do_tb,
                    self.runtime_config.buffer_size,
                    &buffer,
                    &self.runtime_config.log_level,
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
                let filter = RdbFilter::from_config(&self.filter_config)?;
                let extractor = TaskUtil::create_mysql_cdc_extractor(
                    &url,
                    &binlog_filename,
                    *binlog_position,
                    *server_id,
                    &buffer,
                    filter,
                    &self.runtime_config.log_level,
                    &shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::PgSnapshot { url, do_tb } => {
                let extractor = TaskUtil::create_pg_snapshot_extractor(
                    &url,
                    &do_tb,
                    self.runtime_config.buffer_size,
                    &buffer,
                    &self.runtime_config.log_level,
                    &shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::PgCdc {
                url,
                slot_name,
                start_lsn,
            } => {
                let filter = RdbFilter::from_config(&self.filter_config)?;
                let extractor = TaskUtil::create_pg_cdc_extractor(
                    &url,
                    &slot_name,
                    &start_lsn,
                    &buffer,
                    filter,
                    &self.runtime_config.log_level,
                    &shut_down,
                    metric,
                )
                .await?;
                Box::new(extractor)
            }
        };
        Ok(extractor)
    }

    async fn create_sinker<'a>(
        &self,
        buffer: &'a ConcurrentQueue<RowData>,
        shut_down: &'a AtomicBool,
        metric: Arc<Mutex<Metric>>,
    ) -> Result<ParallelSinker<'a>, Error> {
        let router = RdbRouter::from_config(&self.router_config)?;
        let sinker = match &self.sinker_config {
            SinkerConfig::Mysql { url } => {
                TaskUtil::create_mysql_sinker(
                    &url,
                    &buffer,
                    &router,
                    &self.runtime_config,
                    &shut_down,
                    metric,
                )
                .await?
            }

            SinkerConfig::Pg { url } => {
                TaskUtil::create_pg_sinker(
                    &url,
                    &buffer,
                    &router,
                    &self.runtime_config,
                    &shut_down,
                    metric,
                )
                .await?
            }
        };
        Ok(sinker)
    }

    fn init_log4rs(&self) -> Result<(), Error> {
        let mut config_str = String::new();
        File::open(LOG4RS_YAML)?.read_to_string(&mut config_str)?;
        config_str = config_str
            .replace(LOG_DIR_PLACEHODLER, &self.runtime_config.log_dir)
            .replace(LOG_LEVEL_PLACEHODLER, &self.runtime_config.log_level);

        let config: RawConfig = serde_yaml::from_str(&config_str)?;
        log4rs::init_raw_config(config).unwrap();
        Ok(())
    }
}
