use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use log::info;

use crate::{
    error::Error,
    meta::{row_data::RowData, row_type::RowType},
    metric::Metric,
    monitor::{counter::Counter, statistic_counter::StatisticCounter},
    task::task_util::TaskUtil,
    traits::{Pipeline, Sinker},
};

use super::{
    default_pipeline::DefaultPipeline,
    rdb_merger::{RdbMerger, TbMergedData},
};

pub struct MergePipeline<'a> {
    pub buffer: &'a ConcurrentQueue<RowData>,
    pub merger: RdbMerger,
    pub sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    pub shut_down: &'a AtomicBool,
    pub checkpoint_interval_secs: u64,
    pub metric: Arc<Mutex<Metric>>,
}

#[async_trait]
impl Pipeline for MergePipeline<'_> {
    async fn stop(&mut self) -> Result<(), Error> {
        DefaultPipeline::close_sinkers(&mut self.sinkers).await
    }

    async fn start(&mut self) -> Result<(), Error> {
        info!(
            "MergePipeline starts, parallel_size: {}, checkpoint_interval_secs: {}",
            self.sinkers.len(),
            self.checkpoint_interval_secs
        );

        let mut last_checkpoint_time = Instant::now();
        let mut count_counter = Counter::new();
        let mut tps_counter = StatisticCounter::new(self.checkpoint_interval_secs);
        let mut last_row_data = Option::None;

        while !self.shut_down.load(Ordering::Acquire) || !self.buffer.is_empty() {
            let mut all_data = Vec::new();
            while let Ok(row_data) = self.buffer.pop() {
                all_data.push(row_data);
            }

            let count = all_data.len() as u64;
            if all_data.len() > 0 {
                last_row_data = Some(all_data.last().unwrap().clone());
            }

            let mut merged_datas = self.merger.merge(all_data).await?;
            self.sink(&mut merged_datas, "delete").await?;
            self.sink(&mut merged_datas, "insert").await?;
            self.sink(&mut merged_datas, "unmerged").await?;

            last_checkpoint_time = DefaultPipeline::record_checkpoint(
                last_checkpoint_time,
                self.checkpoint_interval_secs,
                &last_row_data,
                &self.metric,
                &mut tps_counter,
                &mut count_counter,
                count,
            );
            TaskUtil::sleep_millis(1).await;
        }
        Ok(())
    }
}

impl MergePipeline<'_> {
    #[inline(always)]
    async fn sink(
        &self,
        merged_datas: &mut HashMap<String, TbMergedData>,
        sink_type: &str,
    ) -> Result<(), Error> {
        let parallel_size = self.sinkers.len();
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
            let sinker = self.sinkers[i % parallel_size].clone();
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
