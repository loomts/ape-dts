use crate::{
    config::{pipeline_config::PipelineType, sinker_config::SinkerConfig, task_config::TaskConfig},
    error::Error,
    pipeline::{
        check_parallelizer::CheckParallelizer, merge_parallelizer::MergeParallelizer,
        partition_parallelizer::PartitionParallelizer, rdb_merger::RdbMerger,
        rdb_partitioner::RdbPartitioner, snapshot_parallelizer::SnapshotParallelizer,
    },
    traits::Parallelizer,
};

use super::task_util::TaskUtil;

pub struct PipelineUtil {}

impl PipelineUtil {
    pub async fn create_parallelizer(
        config: &TaskConfig,
    ) -> Result<Box<dyn Parallelizer + Send>, Error> {
        let parallel_size = config.pipeline.parallel_size;
        let parallelizer_type = &config.pipeline.pipeline_type;

        let parallelizer: Box<dyn Parallelizer + Send> = match parallelizer_type {
            PipelineType::Snapshot => Box::new(SnapshotParallelizer { parallel_size }),

            PipelineType::RdbPartition => {
                let partitioner = Self::create_rdb_partitioner(config).await?;
                Box::new(PartitionParallelizer {
                    partitioner,
                    parallel_size,
                })
            }

            PipelineType::RdbMerge => {
                let merger = Self::create_rdb_merger(config).await?;
                Box::new(MergeParallelizer {
                    merger,
                    parallel_size,
                })
            }

            PipelineType::RdbCheck => {
                let merger = Self::create_rdb_merger(config).await?;
                Box::new(CheckParallelizer {
                    merger,
                    parallel_size,
                })
            }
        };
        Ok(parallelizer)
    }

    pub async fn create_rdb_merger(config: &TaskConfig) -> Result<RdbMerger, Error> {
        let log_level = &config.runtime.log_level;
        let merger = match &config.sinker {
            SinkerConfig::Mysql { url, .. } | SinkerConfig::MysqlCheck { url, .. } => {
                let meta_manager = TaskUtil::create_mysql_meta_manager(&url, log_level).await?;
                RdbMerger::new_for_mysql(meta_manager)
            }

            SinkerConfig::Pg { url, .. } | SinkerConfig::PgCheck { url, .. } => {
                let meta_manager = TaskUtil::create_pg_meta_manager(&url, log_level).await?;
                RdbMerger::new_for_pg(meta_manager)
            }
        };
        Ok(merger)
    }

    pub async fn create_rdb_partitioner(config: &TaskConfig) -> Result<RdbPartitioner, Error> {
        let log_level = &config.runtime.log_level;
        let merger = match &config.sinker {
            SinkerConfig::Mysql { url, .. } | SinkerConfig::MysqlCheck { url, .. } => {
                let meta_manager = TaskUtil::create_mysql_meta_manager(&url, log_level).await?;
                RdbPartitioner::new_for_mysql(meta_manager)
            }

            SinkerConfig::Pg { url, .. } | SinkerConfig::PgCheck { url, .. } => {
                let meta_manager = TaskUtil::create_pg_meta_manager(&url, log_level).await?;
                RdbPartitioner::new_for_pg(meta_manager)
            }
        };
        Ok(merger)
    }
}
