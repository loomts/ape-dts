use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};

use concurrent_queue::ConcurrentQueue;
use log::info;

use crate::{
    common::syncer::Syncer,
    error::Error,
    meta::row_data::RowData,
    monitor::{counter::Counter, statistic_counter::StatisticCounter},
    task::task_util::TaskUtil,
    traits::{Parallelizer, Sinker},
};

pub struct Pipeline<'a> {
    pub buffer: &'a ConcurrentQueue<RowData>,
    pub parallelizer: Box<dyn Parallelizer + Send>,
    pub sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    pub shut_down: &'a AtomicBool,
    pub checkpoint_interval_secs: u64,
    pub syncer: Arc<Mutex<Syncer>>,
}

const POSITION_FILE_LOGGER: &str = "position_file_logger";
const MONITOR_FILE_LOGGER: &str = "monitor_file_logger";

impl Pipeline<'_> {
    pub async fn stop(&mut self) -> Result<(), Error> {
        for sinker in self.sinkers.iter_mut() {
            sinker.lock().await.close().await.unwrap();
        }
        Ok(())
    }

    pub async fn start(&mut self) -> Result<(), Error> {
        info!(
            "{} starts, parallel_size: {}, checkpoint_interval_secs: {}",
            self.parallelizer.get_name(),
            self.sinkers.len(),
            self.checkpoint_interval_secs
        );

        let mut last_checkpoint_time = Instant::now();
        let mut count_counter = Counter::new();
        let mut tps_counter = StatisticCounter::new(self.checkpoint_interval_secs);
        let mut last_row_data = Option::None;

        while !self.shut_down.load(Ordering::Acquire) || !self.buffer.is_empty() {
            // process all row_datas in buffer at a time
            let data = self.parallelizer.drain(&self.buffer).await.unwrap();

            let count = data.len() as u64;
            if count > 0 {
                last_row_data = Some(data.last().unwrap().clone());
                self.parallelizer.sink(data, &self.sinkers).await.unwrap();
            }

            last_checkpoint_time = self.record_checkpoint(
                last_checkpoint_time,
                &last_row_data,
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

impl Pipeline<'_> {
    #[inline(always)]
    pub fn record_checkpoint(
        &self,
        last_checkpoint_time: Instant,
        last_row_data: &Option<RowData>,
        tps_counter: &mut StatisticCounter,
        count_counter: &mut Counter,
        count: u64,
    ) -> Instant {
        tps_counter.add(count);
        count_counter.add(count);

        if last_checkpoint_time.elapsed().as_secs() < self.checkpoint_interval_secs {
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

            self.syncer.lock().unwrap().position = row_data.current_position.clone();
        }

        Instant::now()
    }
}
