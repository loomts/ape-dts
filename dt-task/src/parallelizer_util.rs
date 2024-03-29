use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use dt_common::{
    config::{
        config_enums::{DbType, ParallelType},
        sinker_config::SinkerConfig,
        task_config::TaskConfig,
    },
    error::Error,
    monitor::monitor::Monitor,
};
use dt_meta::redis::command::key_parser::KeyParser;
use dt_parallelizer::{
    base_parallelizer::BaseParallelizer, check_parallelizer::CheckParallelizer,
    merge_parallelizer::MergeParallelizer, mongo_merger::MongoMerger,
    partition_parallelizer::PartitionParallelizer, rdb_merger::RdbMerger,
    rdb_partitioner::RdbPartitioner, redis_parallelizer::RedisParallelizer,
    serial_parallelizer::SerialParallelizer, snapshot_parallelizer::SnapshotParallelizer,
    table_parallelizer::TableParallelizer, Merger, Parallelizer,
};
use ratelimit::Ratelimiter;

use crate::redis_util::RedisUtil;

use super::task_util::TaskUtil;

pub struct ParallelizerUtil {}

impl ParallelizerUtil {
    pub async fn create_parallelizer(
        config: &TaskConfig,
        monitor: Arc<Mutex<Monitor>>,
        rps_limiter: Option<Ratelimiter>,
    ) -> Result<Box<dyn Parallelizer + Send>, Error> {
        let parallel_size = config.parallelizer.parallel_size;
        let parallel_type = &config.parallelizer.parallel_type;
        let base_parallelizer = BaseParallelizer {
            poped_data: VecDeque::new(),
            monitor: monitor.clone(),
            rps_limiter,
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
                let meta_manager = TaskUtil::create_rdb_meta_manager(config).await?;
                Box::new(MergeParallelizer {
                    base_parallelizer,
                    merger,
                    parallel_size,
                    sinker_basic_config: config.sinker_basic.clone(),
                    meta_manager,
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
                    meta_manager: None,
                })
            }

            ParallelType::Redis => {
                let mut slot_node_map = HashMap::new();
                if let SinkerConfig::Redis { is_cluster, .. } = config.sinker {
                    let mut conn = RedisUtil::create_redis_conn(&config.sinker_basic.url).await?;
                    if is_cluster {
                        let (nodes, slots) = RedisUtil::get_cluster_nodes(&mut conn)?;
                        for i in 0..nodes.len() {
                            let node: &'static str = Box::leak(nodes[i].clone().into_boxed_str());
                            for slot in slots[i].iter() {
                                slot_node_map.insert(*slot, node);
                            }
                        }
                    }
                }
                Box::new(RedisParallelizer {
                    base_parallelizer,
                    parallel_size,
                    slot_node_map,
                    key_parser: KeyParser::new(),
                    node_sinker_index_map: HashMap::new(),
                })
            }
        };
        Ok(parallelizer)
    }

    async fn create_rdb_merger(
        config: &TaskConfig,
    ) -> Result<Box<dyn Merger + Send + Sync>, Error> {
        let meta_manager = TaskUtil::create_rdb_meta_manager(config).await?.unwrap();
        let rdb_merger = RdbMerger { meta_manager };
        Ok(Box::new(rdb_merger))
    }

    async fn create_mongo_merger() -> Result<Box<dyn Merger + Send + Sync>, Error> {
        let mongo_merger = MongoMerger {};
        Ok(Box::new(mongo_merger))
    }

    async fn create_rdb_partitioner(config: &TaskConfig) -> Result<RdbPartitioner, Error> {
        let meta_manager = TaskUtil::create_rdb_meta_manager(config).await?.unwrap();
        Ok(RdbPartitioner { meta_manager })
    }
}
