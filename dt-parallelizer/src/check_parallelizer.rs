use std::sync::Arc;

use async_trait::async_trait;
use dt_common::meta::{
    dt_data::DtItem, dt_queue::DtQueue, row_data::RowData, struct_meta::struct_data::StructData,
};
use dt_connector::Sinker;

use crate::{Merger, Parallelizer};

use super::{base_parallelizer::BaseParallelizer, snapshot_parallelizer::SnapshotParallelizer};

pub struct CheckParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub merger: Box<dyn Merger + Send + Sync>,
    pub parallel_size: usize,
}

#[async_trait]
impl Parallelizer for CheckParallelizer {
    fn get_name(&self) -> String {
        "CheckParallelizer".to_string()
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.merger.close().await
    }

    async fn drain(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        self.base_parallelizer.drain(buffer).await
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<()> {
        let mut merged_datas = self.merger.merge(data).await?;
        for tb_merged_data in merged_datas.drain(..) {
            let batch_data = tb_merged_data.insert_rows;
            let batch_sub_datas = SnapshotParallelizer::partition(batch_data, self.parallel_size)?;
            self.base_parallelizer
                .sink_dml(batch_sub_datas, sinkers, self.parallel_size, true)
                .await?;

            let serial_data = tb_merged_data.unmerged_rows;
            let serial_sub_datas =
                SnapshotParallelizer::partition(serial_data, self.parallel_size)?;
            self.base_parallelizer
                .sink_dml(serial_sub_datas, sinkers, self.parallel_size, false)
                .await?;
        }
        Ok(())
    }

    async fn sink_struct(
        &mut self,
        data: Vec<StructData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<()> {
        sinkers[0].lock().await.sink_struct(data).await
    }
}
