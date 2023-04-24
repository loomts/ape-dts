use std::sync::Arc;

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;

use crate::{
    error::Error,
    meta::{ddl_data::DdlData, dt_data::DtData, row_data::RowData},
    traits::{Parallelizer, Sinker},
};

use super::base_parallelizer::BaseParallelizer;

pub struct SnapshotParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub parallel_size: usize,
}

#[async_trait]
impl Parallelizer for SnapshotParallelizer {
    fn get_name(&self) -> String {
        "SnapshotParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &ConcurrentQueue<DtData>) -> Result<Vec<DtData>, Error> {
        self.base_parallelizer.drain(buffer)
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    ) -> Result<(), Error> {
        let sub_datas = Self::partition(data, self.parallel_size)?;
        self.base_parallelizer
            .sink_dml(sub_datas, sinkers, self.parallel_size, true)
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
