use std::collections::VecDeque;

use dt_common::config::{config_enums::ParallelType, task_config::TaskConfig};

use crate::{
    error::Error,
    pipeline::{
        base_parallelizer::BaseParallelizer, check_parallelizer::CheckParallelizer,
        merge_parallelizer::MergeParallelizer, mongo_parallelizer::MongoParallelizer,
        partition_parallelizer::PartitionParallelizer, rdb_merger::RdbMerger,
        rdb_partitioner::RdbPartitioner, serial_parallelizer::SerialParallelizer,
        snapshot_parallelizer::SnapshotParallelizer, table_parallelizer::TableParallelizer,
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
        let base_parallelizer = BaseParallelizer {
            poped_data: VecDeque::new(),
        };

        let parallelizer: Box<dyn Parallelizer + Send> = match parallel_type {
            ParallelType::Snapshot => Box::new(SnapshotParallelizer {
                base_parallelizer,
                parallel_size,
            }),

            ParallelType::RdbPartition => {
                let partitioner = Self::create_rdb_partitioner(config).await?;
                Box::new(PartitionParallelizer {
                    base_parallelizer,
                    partitioner,
                    parallel_size,
                })
            }

            ParallelType::RdbMerge => {
                let merger = Self::create_rdb_merger(config).await?;
                Box::new(MergeParallelizer {
                    base_parallelizer,
                    merger,
                    parallel_size,
                })
            }

            ParallelType::RdbCheck => {
                let merger = Self::create_rdb_merger(config).await?;
                Box::new(CheckParallelizer {
                    base_parallelizer,
                    merger,
                    parallel_size,
                })
            }

            ParallelType::Serial => Box::new(SerialParallelizer { base_parallelizer }),

            ParallelType::Table => Box::new(TableParallelizer {
                base_parallelizer,
                parallel_size,
            }),

            ParallelType::Mongo => Box::new(MongoParallelizer {
                base_parallelizer,
                parallel_size,
            }),
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
