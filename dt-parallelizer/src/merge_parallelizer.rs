use std::{cmp, sync::Arc};

use async_trait::async_trait;
use dt_common::config::sinker_config::BasicSinkerConfig;
use dt_common::meta::ddl_meta::ddl_data::DdlData;
use dt_common::meta::dt_queue::DtQueue;
use dt_common::meta::{
    dt_data::DtItem, rdb_meta_manager::RdbMetaManager, row_data::RowData, row_type::RowType,
};
use dt_connector::Sinker;

use crate::{Merger, Parallelizer};

use super::base_parallelizer::BaseParallelizer;

pub struct MergeParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub merger: Box<dyn Merger + Send + Sync>,
    pub meta_manager: Option<RdbMetaManager>,
    pub parallel_size: usize,
    pub sinker_basic_config: BasicSinkerConfig,
}

enum MergeType {
    Insert,
    Delete,
    Unmerged,
}

pub struct TbMergedData {
    pub tb: String,
    pub delete_rows: Vec<RowData>,
    pub insert_rows: Vec<RowData>,
    pub unmerged_rows: Vec<RowData>,
}

#[async_trait]
impl Parallelizer for MergeParallelizer {
    async fn close(&mut self) -> anyhow::Result<()> {
        if let Some(meta_manager) = &self.meta_manager {
            meta_manager.close().await?;
        }
        self.merger.close().await
    }

    fn get_name(&self) -> String {
        "MergeParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        self.base_parallelizer.drain(buffer).await
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<()> {
        let mut any_fk_tb = false;
        if let Some(rdb_meta_manager) = self.meta_manager.as_mut() {
            for row_data in data.iter() {
                let tb_meta = rdb_meta_manager
                    .get_tb_meta(&row_data.schema, &row_data.tb)
                    .await?;
                if !tb_meta.foreign_keys.is_empty() {
                    any_fk_tb = true;
                    break;
                }
            }
        }

        // do serial sink if any row_data comes from a table with foreign keys
        if any_fk_tb {
            return self
                .base_parallelizer
                .sink_dml(vec![data], sinkers, 1, false)
                .await;
        }

        let mut tb_merged_datas = self.merger.merge(data).await?;
        self.sink_dml_internal(&mut tb_merged_datas, sinkers, MergeType::Delete)
            .await?;
        self.sink_dml_internal(&mut tb_merged_datas, sinkers, MergeType::Insert)
            .await?;
        self.sink_dml_internal(&mut tb_merged_datas, sinkers, MergeType::Unmerged)
            .await
    }

    async fn sink_ddl(
        &mut self,
        data: Vec<DdlData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<()> {
        // ddl should always be excuted serially
        self.base_parallelizer
            .sink_ddl(vec![data], sinkers, 1, false)
            .await
    }
}

impl MergeParallelizer {
    async fn sink_dml_internal(
        &self,
        tb_merged_datas: &mut [TbMergedData],
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
        merge_type: MergeType,
    ) -> anyhow::Result<()> {
        let mut futures = Vec::new();
        for tb_merged_data in tb_merged_datas.iter_mut() {
            let data: Vec<RowData> = match merge_type {
                MergeType::Delete => tb_merged_data.delete_rows.drain(..).collect(),
                MergeType::Insert => tb_merged_data.insert_rows.drain(..).collect(),
                MergeType::Unmerged => tb_merged_data.unmerged_rows.drain(..).collect(),
            };
            if data.is_empty() {
                continue;
            }

            // make sure NO too much threads generated
            let batch_size = cmp::max(
                data.len() / self.parallel_size,
                cmp::max(self.sinker_basic_config.batch_size, 1),
            );

            match merge_type {
                MergeType::Insert | MergeType::Delete => {
                    let mut i = 0;
                    while i < data.len() {
                        let sub_size = cmp::min(batch_size, data.len() - i);
                        let sub_data = data[i..i + sub_size].to_vec();
                        let sinker = sinkers[futures.len() % self.parallel_size].clone();
                        let future = tokio::spawn(async move {
                            sinker.lock().await.sink_dml(sub_data, true).await.unwrap();
                        });
                        futures.push(future);
                        i += batch_size;
                    }
                }

                MergeType::Unmerged => {
                    let sinker = sinkers[futures.len() % self.parallel_size].clone();
                    let future = tokio::spawn(async move {
                        Self::sink_unmerged_rows(sinker, data).await.unwrap();
                    });
                    futures.push(future);
                }
            }
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
    ) -> anyhow::Result<()> {
        let mut start = 0;
        for i in 1..=data.len() {
            if i == data.len() || data[i].row_type != data[start].row_type {
                let sub_data = data[start..i].to_vec();
                if data[start].row_type == RowType::Insert {
                    sinker.lock().await.sink_dml(sub_data, true).await?;
                } else {
                    // for Delete / Update, the safest way is serial
                    sinker.lock().await.sink_dml(sub_data, false).await?;
                }
                start = i;
            }
        }
        Ok(())
    }
}
