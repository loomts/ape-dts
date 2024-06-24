use std::sync::Arc;

use async_trait::async_trait;
use dt_common::meta::{dt_data::DtItem, dt_queue::DtQueue, row_data::RowData};
use dt_connector::Sinker;

use crate::Parallelizer;

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

    async fn drain(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        self.base_parallelizer.drain(buffer).await
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<()> {
        let sub_datas = Self::partition(data, self.parallel_size)?;
        self.base_parallelizer
            .sink_dml(sub_datas, sinkers, self.parallel_size, true)
            .await
    }

    async fn sink_raw(
        &mut self,
        data: Vec<DtItem>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<()> {
        let sub_datas = Self::partition(data, self.parallel_size)?;
        self.base_parallelizer
            .sink_raw(sub_datas, sinkers, self.parallel_size, true)
            .await
    }
}

impl SnapshotParallelizer {
    pub fn partition<T>(data: Vec<T>, parallele_size: usize) -> anyhow::Result<Vec<Vec<T>>> {
        let mut sub_datas = Vec::new();
        if parallele_size <= 1 {
            sub_datas.push(data);
            return Ok(sub_datas);
        }

        let avg_size = data.len() / parallele_size + 1;
        for _ in 0..parallele_size {
            sub_datas.push(Vec::with_capacity(avg_size));
        }

        for (i, item) in data.into_iter().enumerate() {
            sub_datas[i / avg_size].push(item);
        }
        Ok(sub_datas)
    }
}
