use std::sync::Arc;

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;

use crate::{
    error::Error,
    meta::{dt_data::DtData, row_data::RowData},
    traits::{Parallelizer, Sinker},
};

use super::{
    parallelizer_util::ParallelizerUtil, rdb_merger::RdbMerger,
    snapshot_parallelizer::SnapshotParallelizer,
};

pub struct CheckParallelizer {
    pub merger: RdbMerger,
    pub parallel_size: usize,
}

#[async_trait]
impl Parallelizer for CheckParallelizer {
    fn get_name(&self) -> String {
        "CheckParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &ConcurrentQueue<DtData>) -> Result<Vec<DtData>, Error> {
        ParallelizerUtil::drain(buffer)
    }

    async fn sink(
        &mut self,
        data: Vec<RowData>,
        sinkers: &Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    ) -> Result<(), Error> {
        let (batch_sub_datas, serial_sub_datas) = self.partition(data).await?;
        ParallelizerUtil::sink(batch_sub_datas, sinkers, self.parallel_size, true)
            .await
            .unwrap();
        ParallelizerUtil::sink(serial_sub_datas, sinkers, self.parallel_size, false)
            .await
            .unwrap();
        Ok(())
    }
}

impl CheckParallelizer {
    async fn partition(
        &mut self,
        data: Vec<RowData>,
    ) -> Result<(Vec<Vec<RowData>>, Vec<Vec<RowData>>), Error> {
        let full_tb = format!("{}.{}", data[0].db, data[0].tb);
        let mut batch_sub_datas = Vec::new();
        let mut serial_sub_datas = Vec::new();

        // data are all from the same table
        let mut merged_datas = self.merger.merge(data).await?;
        if let Some(tb_merged_data) = merged_datas.get_mut(&full_tb) {
            let batch_data = tb_merged_data.get_insert_rows();
            batch_sub_datas = SnapshotParallelizer::partition(batch_data, self.parallel_size)?;

            let serial_data = tb_merged_data.get_unmerged_rows();
            serial_sub_datas = SnapshotParallelizer::partition(serial_data, self.parallel_size)?;
        }

        Ok((batch_sub_datas, serial_sub_datas))
    }
}
