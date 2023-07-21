use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::error::Error;
use dt_connector::Sinker;
use dt_meta::{ddl_data::DdlData, dt_data::DtData, row_data::RowData, row_type::RowType};

use crate::Parallelizer;

use super::{
    base_parallelizer::BaseParallelizer,
    rdb_merger::{RdbMerger, TbMergedData},
};

pub struct MergeParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub merger: RdbMerger,
    pub parallel_size: usize,
}

const INSERT: &str = "insert";
const DELETE: &str = "delete";
const UNMERGED: &str = "unmerged";

#[async_trait]
impl Parallelizer for MergeParallelizer {
    fn get_name(&self) -> String {
        "MergeParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &ConcurrentQueue<DtData>) -> Result<Vec<DtData>, Error> {
        self.base_parallelizer.drain(buffer)
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> Result<(), Error> {
        let mut merged_datas = self.merger.merge(data).await?;
        self.sink_internal(&mut merged_datas, sinkers, DELETE)
            .await?;
        self.sink_internal(&mut merged_datas, sinkers, INSERT)
            .await?;
        self.sink_internal(&mut merged_datas, sinkers, UNMERGED)
            .await?;
        Ok(())
    }

    async fn sink_ddl(
        &mut self,
        data: Vec<DdlData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> Result<(), Error> {
        // ddl should always be excuted serially
        self.base_parallelizer
            .sink_ddl(vec![data], sinkers, 1, false)
            .await
    }
}

impl MergeParallelizer {
    #[inline(always)]
    async fn sink_internal(
        &self,
        merged_datas: &mut HashMap<String, TbMergedData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
        sink_type: &str,
    ) -> Result<(), Error> {
        let parallel_size = sinkers.len();
        let mut i = 0;
        let mut futures = Vec::new();
        for (_full_tb, tb_merged_data) in merged_datas.iter_mut() {
            let data = match sink_type {
                DELETE => tb_merged_data.get_delete_rows(),
                INSERT => tb_merged_data.get_insert_rows(),
                _ => tb_merged_data.get_unmerged_rows(),
            };
            if data.is_empty() {
                continue;
            }

            let sinker_type_clone = sink_type.to_string();
            let sinker = sinkers[i % parallel_size].clone();
            let future = tokio::spawn(async move {
                match sinker_type_clone.as_str() {
                    DELETE | INSERT => sinker.lock().await.sink_dml(data, true).await.unwrap(),
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
                    sinker.lock().await.sink_dml(sub_data, true).await?;
                } else {
                    sinker.lock().await.sink_dml(sub_data, false).await?;
                }
                start = i;
            }
        }
        Ok(())
    }
}
