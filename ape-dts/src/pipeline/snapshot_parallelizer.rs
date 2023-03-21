use std::sync::Arc;

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;

use crate::{
    error::Error,
    meta::row_data::RowData,
    traits::{Parallelizer, Sinker},
};

use super::parallelizer_util::ParallelizerUtil;

pub struct SnapshotParallelizer {
    pub parallel_size: usize,
}

#[async_trait]
impl Parallelizer for SnapshotParallelizer {
    fn get_name(&self) -> String {
        "SnapshotParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &ConcurrentQueue<RowData>) -> Result<Vec<RowData>, Error> {
        ParallelizerUtil::drain(buffer)
    }

    async fn sink(
        &mut self,
        data: Vec<RowData>,
        sinkers: &Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    ) -> Result<(), Error> {
        let sub_datas = Self::partition(data, self.parallel_size)?;
        ParallelizerUtil::sink(sub_datas, sinkers, self.parallel_size, true).await
    }
}

impl SnapshotParallelizer {
    pub fn partition(
        data: Vec<RowData>,
        parallele_size: usize,
    ) -> Result<Vec<Vec<RowData>>, Error> {
        let mut sub_datas = Vec::new();
        if parallele_size <= 1 {
            sub_datas.push(data);
            return Ok(sub_datas);
        }

        let avg_size = data.len() / parallele_size + 1;
        for _ in 0..parallele_size {
            sub_datas.push(Vec::with_capacity(avg_size));
        }

        let mut i = 0;
        for row_data in data {
            sub_datas[i / avg_size].push(row_data);
            i += 1;
        }
        Ok(sub_datas)
    }
}
