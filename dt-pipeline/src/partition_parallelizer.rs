use std::sync::Arc;

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::error::Error;
use dt_connector::Sinker;
use dt_meta::{ddl_data::DdlData, dt_data::DtData, row_data::RowData};

use crate::Parallelizer;

use super::{base_parallelizer::BaseParallelizer, rdb_partitioner::RdbPartitioner};

pub struct PartitionParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub partitioner: RdbPartitioner,
    pub parallel_size: usize,
}

#[async_trait]
impl Parallelizer for PartitionParallelizer {
    fn get_name(&self) -> String {
        "PartitionParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &ConcurrentQueue<DtData>) -> Result<Vec<DtData>, Error> {
        let mut data = Vec::new();
        while let Ok(dt_data) = buffer.pop() {
            match &dt_data {
                DtData::Dml { row_data } => {
                    if self.parallel_size > 1
                        && !self.partitioner.can_be_partitioned(&row_data).await?
                    {
                        data.push(dt_data);
                        break;
                    } else {
                        data.push(dt_data);
                    }
                }

                DtData::Commit { .. } => {
                    data.push(dt_data);
                }

                _ => {}
            }
        }
        Ok(data)
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    ) -> Result<(), Error> {
        let sub_datas = self.partitioner.partition(data, self.parallel_size).await?;
        self.base_parallelizer
            .sink_dml(sub_datas, sinkers, self.parallel_size, false)
            .await
    }

    async fn sink_ddl(
        &mut self,
        _data: Vec<DdlData>,
        _sinkers: &Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    ) -> Result<(), Error> {
        Ok(())
    }
}
