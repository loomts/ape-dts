use std::sync::Arc;

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::{
    error::Error,
    meta::{ddl_data::DdlData, dt_data::DtData, row_data::RowData},
};
use dt_connector::Sinker;

use crate::Parallelizer;

use super::{
    base_parallelizer::BaseParallelizer, rdb_merger::RdbMerger,
    snapshot_parallelizer::SnapshotParallelizer,
};

pub struct CheckParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub merger: RdbMerger,
    pub parallel_size: usize,
}

#[async_trait]
impl Parallelizer for CheckParallelizer {
    fn get_name(&self) -> String {
        "CheckParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &ConcurrentQueue<DtData>) -> Result<Vec<DtData>, Error> {
        self.base_parallelizer.drain(buffer)
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    ) -> Result<(), Error> {
        let mut merged_datas = self.merger.merge(data).await?;
        for (_full_tb, tb_merged_data) in merged_datas.iter_mut() {
            let batch_data = tb_merged_data.get_insert_rows();
            let batch_sub_datas = SnapshotParallelizer::partition(batch_data, self.parallel_size)?;
            self.base_parallelizer
                .sink_dml(batch_sub_datas, sinkers, self.parallel_size, true)
                .await
                .unwrap();

            let serial_data = tb_merged_data.get_unmerged_rows();
            let serial_sub_datas =
                SnapshotParallelizer::partition(serial_data, self.parallel_size)?;
            self.base_parallelizer
                .sink_dml(serial_sub_datas, sinkers, self.parallel_size, false)
                .await
                .unwrap();
        }
        Ok(())
    }

    async fn sink_ddl(
        &mut self,
        _data: Vec<DdlData>,
        _sinkers: &Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    ) -> Result<(), Error> {
        Ok(())
    }
}
