use std::{
    fs::File,
    io::Read,
    sync::{atomic::AtomicBool, Arc, Mutex},
};

use concurrent_queue::ConcurrentQueue;
use dt_common::{
    config::{extractor_config::ExtractorConfig, task_config::TaskConfig},
    meta::db_enums::DbType,
};
use futures::future::join;
use log4rs::config::RawConfig;

use crate::{
    error::Error,
    extractor::rdb_filter::RdbFilter,
    meta::{row_data::RowData, row_type::RowType},
    metric::Metric,
    pipeline::pipeline::Pipeline,
    traits::Extractor,
};

use super::{extractor_util::ExtractorUtil, pipeline_util::PipelineUtil, sinker_util::SinkerUtil};

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
            ExtractorConfig::MysqlSnapshot { url, .. } => {
                self.start_multi_task(url, &DbType::Mysql).await?
            }

            ExtractorConfig::PgSnapshot { url, .. } => {
                self.start_multi_task(url, &DbType::Pg).await?
            }

            _ => self.start_single_task(&self.config.extractor).await?,
        };

        Ok(())
    }

    async fn start_multi_task(&self, url: &str, db_type: &DbType) -> Result<(), Error> {
        let mut filter = RdbFilter::from_config(&self.config.filter)?;
        let dbs = ExtractorUtil::list_dbs(url, db_type).await?;
        for db in dbs.iter() {
            if filter.filter_db(db) {
                continue;
            }

            let tbs = ExtractorUtil::list_tbs(url, db, db_type).await?;
            for tb in tbs.iter() {
                if filter.filter(db, tb, &RowType::Insert) {
                    continue;
                }

                let single_extractor_config = match &self.config.extractor {
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

                    _ => {
                        return Err(Error::Unexpected {
                            error: "unexpected extractor config type for rdb snapshot task"
                                .to_string(),
                        });
                    }
                };

                self.start_single_task(&single_extractor_config).await?;
            }
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

        let sinkers = SinkerUtil::create_sinkers(&self.config).await?;
        let parallelizer = PipelineUtil::create_parallelizer(&self.config).await?;
        let mut pipeline = Pipeline {
            buffer: &buffer,
            parallelizer,
            sinkers,
            shut_down: &shut_down,
            checkpoint_interval_secs: self.config.pipeline.checkpoint_interval_secs,
            metric,
        };

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
            ExtractorConfig::MysqlSnapshot { url, db, tb } => {
                let extractor = ExtractorUtil::create_mysql_snapshot_extractor(
                    url,
                    db,
                    tb,
                    self.config.pipeline.buffer_size,
                    &buffer,
                    &self.config.runtime.log_level,
                    &shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::MysqlCheck { url, check_log_dir } => {
                let extractor = ExtractorUtil::create_mysql_check_extractor(
                    &url,
                    &check_log_dir,
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

            ExtractorConfig::PgSnapshot { url, db, tb } => {
                let extractor = ExtractorUtil::create_pg_snapshot_extractor(
                    url,
                    db,
                    tb,
                    self.config.pipeline.buffer_size,
                    &buffer,
                    &self.config.runtime.log_level,
                    &shut_down,
                )
                .await?;
                Box::new(extractor)
            }

            ExtractorConfig::PgCheck { url, check_log_dir } => {
                let extractor = ExtractorUtil::create_pg_check_extractor(
                    &url,
                    &check_log_dir,
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

            _ => {
                return Err(Error::Unexpected {
                    error: "unexpected extractor type".to_string(),
                });
            }
        };
        Ok(extractor)
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
