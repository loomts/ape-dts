use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;

use crate::{
    error::Error,
    meta::{row_data::RowData, row_type::RowType},
    traits::{Parallelizer, Sinker},
};

use super::{
    parallelizer_util::ParallelizerUtil,
    rdb_merger::{RdbMerger, TbMergedData},
};

pub struct MergeParallelizer {
    pub merger: RdbMerger,
    pub parallel_size: usize,
}

#[async_trait]
impl Parallelizer for MergeParallelizer {
    fn get_name(&self) -> String {
        "MergeParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &ConcurrentQueue<RowData>) -> Result<Vec<RowData>, Error> {
        ParallelizerUtil::drain(buffer)
    }

    async fn sink(
        &mut self,
        data: Vec<RowData>,
        sinkers: &Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    ) -> Result<(), Error> {
        let mut merged_datas = self.merger.merge(data).await?;
        self.sink_internal(&mut merged_datas, sinkers, "delete")
            .await?;
        self.sink_internal(&mut merged_datas, sinkers, "insert")
            .await?;
        self.sink_internal(&mut merged_datas, sinkers, "unmerged")
            .await?;
        Ok(())
    }
}

impl MergeParallelizer {
    #[inline(always)]
    async fn sink_internal(
        &self,
        merged_datas: &mut HashMap<String, TbMergedData>,
        sinkers: &Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
        sink_type: &str,
    ) -> Result<(), Error> {
        let parallel_size = sinkers.len();
        let mut i = 0;
        let mut futures = Vec::new();
        for (_full_tb, tb_merged_data) in merged_datas.iter_mut() {
            let data = match sink_type {
                "delete" => tb_merged_data.get_delete_rows(),
                "insert" => tb_merged_data.get_insert_rows(),
                _ => tb_merged_data.get_unmerged_rows(),
            };
            if data.len() == 0 {
                continue;
            }

            let sinker_type_clone = sink_type.to_string();
            let sinker = sinkers[i % parallel_size].clone();
            let future = tokio::spawn(async move {
                match sinker_type_clone.as_str() {
                    "delete" | "insert" => sinker.lock().await.batch_sink(data).await.unwrap(),
                    _ => Self::sink_unmerged_rows(sinker, data).await.unwrap(),
                };
            });
            futures.push(future);
            i += 1;
        }

        // wait for sub sinkers to finish and unwrap errors
        for future in futures {
            future.await.unwrap();
        }
        Ok(())
    }

    async fn sink_unmerged_rows(
        sinker: Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>,
        data: Vec<RowData>,
    ) -> Result<(), Error> {
        let mut start = 0;
        for i in 1..=data.len() {
            if i == data.len() || data[i].row_type != data[start].row_type {
                let sub_data = data[start..i].to_vec();
                if data[start].row_type == RowType::Insert {
                    sinker.lock().await.batch_sink(sub_data).await?;
                } else {
                    sinker.lock().await.sink(sub_data).await?;
                }
                start = i;
            }
        }
        Ok(())
    }
}
