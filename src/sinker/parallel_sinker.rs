use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use concurrent_queue::ConcurrentQueue;
use log::info;

use crate::{
    error::Error,
    meta::row_data::RowData,
    metric::Metric,
    monitor::statistic_counter::StatisticCounter,
    task::task_util::TaskUtil,
    traits::{Partitioner, Sinker},
};

pub struct ParallelSinker<'a> {
    pub buffer: &'a ConcurrentQueue<RowData>,
    pub partitioner: Box<dyn Partitioner + Send>,
    pub sub_sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    pub shut_down: &'a AtomicBool,
    pub metric: Arc<Mutex<Metric>>,
}

const POSITION_FILE_LOGGER: &str = "position_file_logger";
const MONITOR_FILE_LOGGER: &str = "monitor_file_logger";
const CHECKPOINT_INTERVAL_SECS: u64 = 60;

impl ParallelSinker<'_> {
    pub async fn close(&mut self) -> Result<(), Error> {
        for sinker in self.sub_sinkers.iter_mut() {
            sinker.lock().await.close().await.unwrap();
        }
        Ok(())
    }

    pub async fn sink(&mut self) -> Result<(), Error> {
        let mut counter = StatisticCounter::new(CHECKPOINT_INTERVAL_SECS);
        let partition_count = self.sub_sinkers.len();

        while !self.shut_down.load(Ordering::Acquire) || !self.buffer.is_empty() {
            // process all row_datas in buffer at a time
            let mut all_data = Vec::new();
            while let Ok(row_data) = self.buffer.pop() {
                // if the row_data can not be partitioned, sink the pushed data immediately
                if !self.partitioner.can_be_partitioned(&row_data).await? {
                    all_data.push(row_data);
                    break;
                } else {
                    all_data.push(row_data);
                }
            }

            let count = all_data.len();
            // record the last row_data for logging position_info
            let mut last_row_data = Option::None;
            if !all_data.is_empty() {
                last_row_data = Some(all_data[all_data.len() - 1].clone());
            }

            // partition data
            let mut sub_datas = self
                .partitioner
                .partition(all_data, partition_count)
                .await?;

            let mut futures = Vec::new();
            for i in 0..partition_count {
                let data = sub_datas.remove(0);
                let sinker = self.sub_sinkers[i].clone();
                futures.push(tokio::spawn(async move {
                    sinker.lock().await.sink(data).await.unwrap();
                }));
            }

            // wait for sub sinkers to finish and unwrap errors
            for future in futures {
                future.await.unwrap();
            }

            self.record_position(&last_row_data);
            counter.add(count as u64);
            self.record_counters(&mut counter);

            // sleep 1 millis for data preparing
            TaskUtil::sleep_millis(1).await;
        }

        Ok(())
    }

    #[inline(always)]
    fn record_position(&self, last_row_data: &Option<RowData>) {
        if let Some(row_data) = last_row_data {
            info!(
                target: POSITION_FILE_LOGGER,
                "current_position | {}", row_data.current_position
            );
            info!(
                target: POSITION_FILE_LOGGER,
                "checkpoint_position | {}", row_data.checkpoint_position
            );
            self.metric.lock().unwrap().position = row_data.current_position.clone();
        }
    }

    fn record_counters(&self, counter: &mut StatisticCounter) {
        info!(
            target: MONITOR_FILE_LOGGER,
            "avg tps in {} seconds: {}",
            CHECKPOINT_INTERVAL_SECS,
            counter.avg()
        );
    }
}
