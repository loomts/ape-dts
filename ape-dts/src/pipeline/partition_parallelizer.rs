use std::sync::Arc;

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;

use crate::{
    error::Error,
    meta::row_data::RowData,
    traits::{Parallelizer, Sinker},
};

use super::{parallelizer_util::ParallelizerUtil, rdb_partitioner::RdbPartitioner};

pub struct PartitionParallelizer {
    pub partitioner: RdbPartitioner,
    pub parallel_size: usize,
}

#[async_trait]
impl Parallelizer for PartitionParallelizer {
    fn get_name(&self) -> String {
        "DefaultParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &ConcurrentQueue<RowData>) -> Result<Vec<RowData>, Error> {
        let mut data = Vec::new();
        while let Ok(row_data) = buffer.pop() {
            // if the row_data can not be partitioned, sink the pushed data immediately
            if self.parallel_size > 1 && !self.partitioner.can_be_partitioned(&row_data).await? {
                data.push(row_data);
                break;
            } else {
                data.push(row_data);
            }
        }
        Ok(data)
    }

    async fn sink(
        &mut self,
        data: Vec<RowData>,
        sinkers: &Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    ) -> Result<(), Error> {
        let sub_datas = self.partitioner.partition(data, self.parallel_size).await?;
        ParallelizerUtil::sink(sub_datas, sinkers, self.parallel_size, false).await
    }
}
