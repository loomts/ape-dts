use dt_common::config::{config_enums::ParallelType, task_config::TaskConfig};

use crate::{
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
        let parallel_type = &config.pipeline.parallel_type;

        let parallelizer: Box<dyn Parallelizer + Send> = match parallel_type {
            ParallelType::Snapshot => Box::new(SnapshotParallelizer { parallel_size }),

            ParallelType::RdbPartition => {
                let partitioner = Self::create_rdb_partitioner(config).await?;
                Box::new(PartitionParallelizer {
                    partitioner,
                    parallel_size,
                })
            }

            ParallelType::RdbMerge => {
                let merger = Self::create_rdb_merger(config).await?;
                Box::new(MergeParallelizer {
                    merger,
                    parallel_size,
                })
            }

            ParallelType::RdbCheck => {
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
        let meta_manager = TaskUtil::create_rdb_meta_manager(config).await?;
        Ok(RdbMerger { meta_manager })
    }

    pub async fn create_rdb_partitioner(config: &TaskConfig) -> Result<RdbPartitioner, Error> {
        let meta_manager = TaskUtil::create_rdb_meta_manager(config).await?;
        Ok(RdbPartitioner { meta_manager })
    }
}
