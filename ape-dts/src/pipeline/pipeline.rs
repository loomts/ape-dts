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
    meta::{dt_data::DtData, row_data::RowData},
    monitor::{counter::Counter, statistic_counter::StatisticCounter},
    task::task_util::TaskUtil,
    traits::{Parallelizer, Sinker},
};

pub struct Pipeline<'a> {
    pub buffer: &'a ConcurrentQueue<DtData>,
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
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;

        while !self.shut_down.load(Ordering::Acquire) || !self.buffer.is_empty() {
            // process all row_datas in buffer at a time
            let all_data = self.parallelizer.drain(&self.buffer).await.unwrap();
            let mut count = 0;
            if all_data.len() > 0 {
                let (data, last_received, last_commit) = Self::filter_data(all_data);
                last_received_position = last_received;
                if last_commit.is_some() {
                    last_commit_position = last_commit;
                }

                count = data.len();
                if count > 0 {
                    self.parallelizer.sink(data, &self.sinkers).await.unwrap();
                }
            }

            last_checkpoint_time = self.record_checkpoint(
                last_checkpoint_time,
                &last_received_position,
                &last_commit_position,
                &mut tps_counter,
                &mut count_counter,
                count as u64,
            );

            // sleep 1 millis for data preparing
            TaskUtil::sleep_millis(1).await;
        }

        Ok(())
    }

    fn filter_data(mut data: Vec<DtData>) -> (Vec<RowData>, Option<String>, Option<String>) {
        let mut filtered_data = Vec::new();
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;
        for i in data.drain(..) {
            match i {
                DtData::Commit { position, .. } => {
                    last_commit_position = Some(position);
                    last_received_position = last_commit_position.clone();
                    continue;
                }

                DtData::Dml { row_data } => {
                    last_received_position = Some(row_data.position.clone());
                    filtered_data.push(row_data);
                }
            }
        }

        (filtered_data, last_received_position, last_commit_position)
    }
}

impl Pipeline<'_> {
    #[inline(always)]
    pub fn record_checkpoint(
        &self,
        last_checkpoint_time: Instant,
        last_received: &Option<String>,
        last_commit: &Option<String>,
        tps_counter: &mut StatisticCounter,
        count_counter: &mut Counter,
        count: u64,
    ) -> Instant {
        tps_counter.add(count);
        count_counter.add(count);

        if last_checkpoint_time.elapsed().as_secs() < self.checkpoint_interval_secs {
            return last_checkpoint_time;
        }

        if let Some(position) = last_received {
            info!(
                target: POSITION_FILE_LOGGER,
                "current_position | {}", position
            );
        }

        if let Some(position) = last_commit {
            info!(
                target: POSITION_FILE_LOGGER,
                "checkpoint_position | {}", position
            );
            self.syncer.lock().unwrap().checkpoint_position = position.clone();
        }

        info!(
            target: MONITOR_FILE_LOGGER,
            "avg tps: {}",
            tps_counter.avg(),
        );

        info!(
            target: MONITOR_FILE_LOGGER,
            "sinked count: {}", count_counter.value
        );

        Instant::now()
    }
}
