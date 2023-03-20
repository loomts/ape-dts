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
    traits::{Partitioner, Pipeline, Sinker},
};

pub struct DefaultPipeline<'a> {
    pub buffer: &'a ConcurrentQueue<RowData>,
    pub partitioner: Box<dyn Partitioner + Send>,
    pub sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    pub shut_down: &'a AtomicBool,
    pub checkpoint_interval_secs: u64,
    pub metric: Arc<Mutex<Metric>>,
}

const POSITION_FILE_LOGGER: &str = "position_file_logger";
const MONITOR_FILE_LOGGER: &str = "monitor_file_logger";

#[async_trait]
impl Pipeline for DefaultPipeline<'_> {
    async fn stop(&mut self) -> Result<(), Error> {
        Self::close_sinkers(&mut self.sinkers).await
    }

    async fn start(&mut self) -> Result<(), Error> {
        info!(
            "DefaultPipeline starts, parallel_size: {}, checkpoint_interval_secs: {}",
            self.sinkers.len(),
            self.checkpoint_interval_secs
        );

        let parallel_size = self.sinkers.len();
        let mut last_checkpoint_time = Instant::now();
        let mut count_counter = Counter::new();
        let mut tps_counter = StatisticCounter::new(self.checkpoint_interval_secs);
        let mut last_row_data = Option::None;

        while !self.shut_down.load(Ordering::Acquire) || !self.buffer.is_empty() {
            // process all row_datas in buffer at a time
            let mut all_data = Vec::new();
            while let Ok(row_data) = self.buffer.pop() {
                // if the row_data can not be partitioned, sink the pushed data immediately
                if parallel_size > 1 && !self.partitioner.can_be_partitioned(&row_data).await? {
                    all_data.push(row_data);
                    break;
                } else {
                    all_data.push(row_data);
                }
            }

            let count = all_data.len() as u64;
            if all_data.len() > 0 {
                last_row_data = Some(all_data.last().unwrap().clone());
            }

            // partition data
            let mut sub_datas = self.partitioner.partition(all_data, parallel_size).await?;
            let mut futures = Vec::new();
            for i in 0..parallel_size {
                let data = sub_datas.remove(0);
                let sinker = self.sinkers[i].clone();
                futures.push(tokio::spawn(async move {
                    sinker.lock().await.sink(data).await.unwrap();
                }));
            }

            // wait for sinkers to finish and unwrap errors
            for future in futures {
                future.await.unwrap();
            }

            last_checkpoint_time = Self::record_checkpoint(
                last_checkpoint_time,
                self.checkpoint_interval_secs,
                &last_row_data,
                &self.metric,
                &mut tps_counter,
                &mut count_counter,
                count,
            );
            // sleep 1 millis for data preparing
            TaskUtil::sleep_millis(1).await;
        }

        Ok(())
    }
}

impl DefaultPipeline<'_> {
    pub async fn close_sinkers(
        sinkers: &mut Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    ) -> Result<(), Error> {
        for sinker in sinkers.iter_mut() {
            sinker.lock().await.close().await.unwrap();
        }
        Ok(())
    }

    #[inline(always)]
    pub fn record_checkpoint(
        last_checkpoint_time: Instant,
        checkpoint_interval_secs: u64,
        last_row_data: &Option<RowData>,
        metric: &Arc<Mutex<Metric>>,
        tps_counter: &mut StatisticCounter,
        count_counter: &mut Counter,
        count: u64,
    ) -> Instant {
        tps_counter.add(count);
        count_counter.add(count);

        if last_checkpoint_time.elapsed().as_secs() < checkpoint_interval_secs {
            return last_checkpoint_time;
        }

        if let Some(row_data) = last_row_data {
            info!(
                target: POSITION_FILE_LOGGER,
                "current_position | {}", row_data.current_position
            );

            info!(
                target: POSITION_FILE_LOGGER,
                "checkpoint_position | {}", row_data.checkpoint_position
            );

            info!(
                target: MONITOR_FILE_LOGGER,
                "avg tps: {}",
                tps_counter.avg(),
            );

            info!(
                target: MONITOR_FILE_LOGGER,
                "sinked count: {}", count_counter.value
            );

            metric.lock().unwrap().position = row_data.current_position.clone();
        }

        Instant::now()
    }
}
