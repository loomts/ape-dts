use std::{collections::VecDeque, sync::Arc};

use async_rwlock::RwLock;
use dt_common::{
    config::{
        config_enums::{DbType, ParallelType},
        task_config::TaskConfig,
    },
    error::Error,
    monitor::monitor::Monitor,
};
use dt_parallelizer::{
    base_parallelizer::BaseParallelizer, check_parallelizer::CheckParallelizer,
    merge_parallelizer::MergeParallelizer, mongo_merger::MongoMerger,
    partition_parallelizer::PartitionParallelizer, rdb_merger::RdbMerger,
    rdb_partitioner::RdbPartitioner, redis_parallelizer::RedisParallelizer,
    serial_parallelizer::SerialParallelizer, snapshot_parallelizer::SnapshotParallelizer,
    table_parallelizer::TableParallelizer, Merger, Parallelizer,
};

use super::task_util::TaskUtil;

pub struct ParallelizerUtil {}

impl ParallelizerUtil {
    pub async fn create_parallelizer(
        config: &TaskConfig,
        monitor: Arc<RwLock<Monitor>>,
    ) -> Result<Box<dyn Parallelizer + Send>, Error> {
        let parallel_size = config.parallelizer.parallel_size;
        let parallel_type = &config.parallelizer.parallel_type;
        let base_parallelizer = BaseParallelizer {
            poped_data: VecDeque::new(),
            monitor: monitor.clone(),
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
                    sinker_basic_config: config.sinker_basic.clone(),
                })
            }

            ParallelType::RdbCheck => {
                let merger = match config.sinker_basic.db_type {
                    DbType::Mongo => Self::create_mongo_merger().await?,
                    _ => Self::create_rdb_merger(config).await?,
                };
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

            ParallelType::Mongo => {
                let merger = Box::new(MongoMerger {});
                Box::new(MergeParallelizer {
                    base_parallelizer,
                    merger,
                    parallel_size,
                    sinker_basic_config: config.sinker_basic.clone(),
                })
            }

            ParallelType::Redis => Box::new(RedisParallelizer {
                base_parallelizer,
                parallel_size,
            }),
        };
        Ok(parallelizer)
    }

    pub async fn create_rdb_merger(
        config: &TaskConfig,
    ) -> Result<Box<dyn Merger + Send + Sync>, Error> {
        let meta_manager = TaskUtil::create_rdb_meta_manager(config).await?;
        let rdb_merger = RdbMerger { meta_manager };
        Ok(Box::new(rdb_merger))
    }

    pub async fn create_mongo_merger() -> Result<Box<dyn Merger + Send + Sync>, Error> {
        let mongo_merger = MongoMerger {};
        Ok(Box::new(mongo_merger))
    }

    pub async fn create_rdb_partitioner(config: &TaskConfig) -> Result<RdbPartitioner, Error> {
        let meta_manager = TaskUtil::create_rdb_meta_manager(config).await?;
        Ok(RdbPartitioner { meta_manager })
    }
}
