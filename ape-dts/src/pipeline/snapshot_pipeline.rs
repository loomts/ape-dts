use std::{
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
    meta::row_data::RowData,
    metric::Metric,
    monitor::{counter::Counter, statistic_counter::StatisticCounter},
    task::task_util::TaskUtil,
    traits::{Pipeline, Sinker},
};

use super::default_pipeline::DefaultPipeline;

pub struct SnapshotPipeline<'a> {
    pub buffer: &'a ConcurrentQueue<RowData>,
    pub sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    pub shut_down: &'a AtomicBool,
    pub checkpoint_interval_secs: u64,
    pub metric: Arc<Mutex<Metric>>,
}

#[async_trait]
impl Pipeline for SnapshotPipeline<'_> {
    async fn stop(&mut self) -> Result<(), Error> {
        DefaultPipeline::close_sinkers(&mut self.sinkers).await
    }

    async fn start(&mut self) -> Result<(), Error> {
        info!(
            "SnapshotPipeline starts, parallel_size: {}, checkpoint_interval_secs: {}",
            self.sinkers.len(),
            self.checkpoint_interval_secs
        );

        let parallel_size = self.sinkers.len();
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

            let mut sub_datas = self.partition(all_data, parallel_size).await?;
            let mut futures = Vec::new();
            for i in 0..parallel_size {
                let data = sub_datas.remove(0);
                let sinker = self.sinkers[i].clone();
                futures.push(tokio::spawn(async move {
                    sinker.lock().await.batch_sink(data).await.unwrap();
                }));
            }

            for future in futures {
                future.await.unwrap();
            }

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

impl SnapshotPipeline<'_> {
    async fn partition(
        &mut self,
        data: Vec<RowData>,
        partition_count: usize,
    ) -> Result<Vec<Vec<RowData>>, Error> {
        let mut sub_datas = Vec::new();
        if partition_count <= 1 {
            sub_datas.push(data);
            return Ok(sub_datas);
        }

        let avg_size = data.len() / partition_count + 1;
        for _ in 0..partition_count {
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
