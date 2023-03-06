use std::{fs::File, io::Read, sync::atomic::AtomicBool};

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
    sinker::{parallel_sinker::ParallelSinker, rdb_router::RdbRouter},
    traits::traits::Extractor,
};

use super::task_util::TaskUtil;

pub struct TaskRunner {}

const LOG_LEVEL_PLACEHODLER: &str = "LOG_LEVEL_PLACEHODLER";
const LOG_DIR_PLACEHODLER: &str = "LOG_DIR_PLACEHODLER";
const LOG4RS_YAML: &str = "log4rs.yaml";

impl TaskRunner {
    pub async fn start_task(task_config: String) -> Result<(), Error> {
        let (extractor_config, sinker_config, runtime_config, filter_config, router_config) =
            ConfigLoader::load(&task_config)?;
        Self::init_log4rs(&runtime_config.log_dir, &runtime_config.log_level)?;

        match &extractor_config {
            ExtractorConfig::MysqlSnapshot { .. } | ExtractorConfig::PgSnapshot { .. } => {
                Self::start_multi_task(
                    &extractor_config,
                    &sinker_config,
                    &runtime_config,
                    &filter_config,
                    &router_config,
                )
                .await?
            }

            _ => {
                Self::start_single_task(
                    &extractor_config,
                    &sinker_config,
                    &runtime_config,
                    &filter_config,
                    &router_config,
                )
                .await?
            }
        };

        Ok(())
    }

    async fn start_multi_task(
        extractor_config: &ExtractorConfig,
        sinker_config: &SinkerConfig,
        runtime_config: &RuntimeConfig,
        filter_config: &FilterConfig,
        router_config: &RouterConfig,
    ) -> Result<(), Error> {
        let filter = RdbFilter::from_config(&filter_config)?;
        for do_tb in filter.do_tbs.iter() {
            let single_extractor_config = match extractor_config {
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

            Self::start_single_task(
                &single_extractor_config,
                &sinker_config,
                &runtime_config,
                &filter_config,
                &router_config,
            )
            .await?;
        }

        Ok(())
    }

    async fn start_single_task(
        extractor_config: &ExtractorConfig,
        sinker_config: &SinkerConfig,
        runtime_config: &RuntimeConfig,
        filter_config: &FilterConfig,
        router_config: &RouterConfig,
    ) -> Result<(), Error> {
        let buffer = ConcurrentQueue::bounded(runtime_config.buffer_size);
        let shut_down = AtomicBool::new(false);

        let mut extractor = Self::create_extractor(
            &runtime_config,
            &extractor_config,
            &filter_config,
            &buffer,
            &shut_down,
        )
        .await?;

        let mut sinker = Self::create_sinker(
            &runtime_config,
            &sinker_config,
            &router_config,
            &buffer,
            &shut_down,
        )
        .await?;

        let result = join(extractor.extract(), sinker.sink()).await;
        sinker.close().await?;
        extractor.close().await?;
        if result.0.is_err() {
            return result.0;
        }
        result.1
    }

    async fn create_extractor<'a>(
        runtime_config: &RuntimeConfig,
        extractor_config: &ExtractorConfig,
        filter_config: &FilterConfig,
        buffer: &'a ConcurrentQueue<RowData>,
        shut_down: &'a AtomicBool,
    ) -> Result<Box<dyn Extractor + 'a + Send>, Error> {
        let extractor: Box<dyn Extractor + Send> = match extractor_config {
            ExtractorConfig::MysqlSnapshot { url, do_tb } => {
                let extractor = TaskUtil::create_mysql_snapshot_extractor(
                    &url,
                    &do_tb,
                    runtime_config.buffer_size,
                    &buffer,
                    &runtime_config.log_level,
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
                let filter = RdbFilter::from_config(filter_config)?;
                let extractor = TaskUtil::create_mysql_cdc_extractor(
                    &url,
                    &binlog_filename,
                    *binlog_position,
                    *server_id,
                    &buffer,
                    filter,
                    &runtime_config.log_level,
                    &shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::PgSnapshot { url, do_tb } => {
                let extractor = TaskUtil::create_pg_snapshot_extractor(
                    &url,
                    &do_tb,
                    runtime_config.buffer_size,
                    &buffer,
                    &runtime_config.log_level,
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
                let filter = RdbFilter::from_config(filter_config)?;
                let extractor = TaskUtil::create_pg_cdc_extractor(
                    &url,
                    &slot_name,
                    &start_lsn,
                    &buffer,
                    filter,
                    &runtime_config.log_level,
                    &shut_down,
                )
                .await?;
                Box::new(extractor)
            }
        };
        Ok(extractor)
    }

    async fn create_sinker<'a>(
        runtime_config: &RuntimeConfig,
        sinker_config: &SinkerConfig,
        router_config: &RouterConfig,
        buffer: &'a ConcurrentQueue<RowData>,
        shut_down: &'a AtomicBool,
    ) -> Result<ParallelSinker<'a>, Error> {
        let router = RdbRouter::from_config(router_config)?;
        let sinker = match sinker_config {
            SinkerConfig::Mysql { url } => {
                TaskUtil::create_mysql_sinker(
                    &url,
                    &buffer,
                    &router,
                    runtime_config.parallel_size,
                    runtime_config.batch_size,
                    &runtime_config.log_level,
                    &shut_down,
                )
                .await?
            }

            SinkerConfig::Pg { url } => {
                TaskUtil::create_pg_sinker(
                    &url,
                    &buffer,
                    &router,
                    runtime_config.parallel_size,
                    runtime_config.batch_size,
                    &runtime_config.log_level,
                    &shut_down,
                )
                .await?
            }
        };
        Ok(sinker)
    }

    fn init_log4rs(log_dir: &str, log_level: &str) -> Result<(), Error> {
        let mut config_str = String::new();
        File::open(LOG4RS_YAML)?.read_to_string(&mut config_str)?;
        config_str = config_str
            .replace(LOG_DIR_PLACEHODLER, log_dir)
            .replace(LOG_LEVEL_PLACEHODLER, log_level);

        let config: RawConfig = serde_yaml::from_str(&config_str)?;
        log4rs::init_raw_config(config).unwrap();
        Ok(())
    }
}
