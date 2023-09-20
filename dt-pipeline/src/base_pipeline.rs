use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::{
    config::{config_enums::DbType, sinker_config::SinkerBasicConfig},
    error::Error,
    log_info, log_monitor, log_position,
    monitor::{counter::Counter, statistic_counter::StatisticCounter},
    syncer::Syncer,
    utils::time_util::TimeUtil,
};
use dt_connector::Sinker;
use dt_meta::dt_data::DtData;
use dt_parallelizer::Parallelizer;

use crate::{drainers::traits::DataDrainer, Pipeline};

pub struct BasePipeline {
    pub buffer: Arc<ConcurrentQueue<DtData>>,
    pub parallelizer: Box<dyn Parallelizer + Send>,
    pub sinker_basic_config: SinkerBasicConfig,
    pub sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    pub shut_down: Arc<AtomicBool>,
    pub checkpoint_interval_secs: u64,
    pub batch_sink_interval_secs: u64,
    pub syncer: Arc<Mutex<Syncer>>,

    pub drainer: Box<dyn DataDrainer + Send>,
}

#[async_trait]
impl Pipeline for BasePipeline {
    async fn stop(&mut self) -> Result<(), Error> {
        for sinker in self.sinkers.iter_mut() {
            sinker.lock().await.close().await.unwrap();
        }
        Ok(())
    }

    async fn start(&mut self) -> Result<(), Error> {
        log_info!(
            "{} starts, parallel_size: {}, checkpoint_interval_secs: {}",
            self.parallelizer.get_name(),
            self.sinkers.len(),
            self.checkpoint_interval_secs
        );

        let mut last_sink_time = Instant::now();
        let mut last_checkpoint_time = Instant::now();
        let mut count_counter = Counter::new();
        let mut tps_counter = StatisticCounter::new(self.checkpoint_interval_secs);
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;

        while !self.shut_down.load(Ordering::Acquire) || !self.buffer.is_empty() {
            // some sinkers (foxlake) need to accumulate data to a big batch and sink
            let data = if last_sink_time.elapsed().as_secs() < self.batch_sink_interval_secs
                && !self.buffer.is_full()
            {
                Vec::new()
            } else {
                last_sink_time = Instant::now();
                self.parallelizer.drain(self.buffer.as_ref()).await.unwrap()
            };

            // process all row_datas in buffer at a time
            let mut sink_count = 0;
            if !data.is_empty() {
                let (count, last_received, last_commit);
                let sink_raw = match self.sinker_basic_config.db_type {
                    DbType::Redis | DbType::Kafka => true,
                    _ => false,
                };
                if sink_raw {
                    (count, last_received, last_commit) = self.sink_raw(data).await.unwrap();
                } else if data[0].is_ddl() {
                    (count, last_received, last_commit) = self.sink_ddl(data).await.unwrap();
                } else {
                    (count, last_received, last_commit) = self.sink_dml(data).await.unwrap();
                }

                sink_count = count;
                last_received_position = last_received;
                if last_commit.is_some() {
                    last_commit_position = last_commit;
                }
            }

            last_checkpoint_time = self.record_checkpoint(
                last_checkpoint_time,
                &last_received_position,
                &last_commit_position,
                &mut tps_counter,
                &mut count_counter,
                sink_count as u64,
            );

            // sleep 1 millis for data preparing
            TimeUtil::sleep_millis(1).await;
        }

        Ok(())
    }
}

impl BasePipeline {
    async fn sink_raw(
        &mut self,
        all_data: Vec<DtData>,
    ) -> Result<(usize, Option<String>, Option<String>), Error> {
        let (data, last_received_position, last_commit_position) =
            self.drainer.drain_raw(all_data)?;
        let count = data.len();
        if count > 0 {
            self.parallelizer
                .sink_raw(data, &self.sinkers)
                .await
                .unwrap()
        }
        Ok((count, last_received_position, last_commit_position))
    }

    async fn sink_dml(
        &mut self,
        all_data: Vec<DtData>,
    ) -> Result<(usize, Option<String>, Option<String>), Error> {
        let (data, last_received_position, last_commit_position) =
            self.drainer.drain_dmls(all_data)?;
        let count = data.len();
        if count > 0 {
            self.parallelizer
                .sink_dml(data, &self.sinkers)
                .await
                .unwrap()
        }
        Ok((count, last_received_position, last_commit_position))
    }

    async fn sink_ddl(
        &mut self,
        all_data: Vec<DtData>,
    ) -> Result<(usize, Option<String>, Option<String>), Error> {
        let (data, last_received_position, last_commit_position) =
            self.drainer.drain_ddl(all_data)?;
        let count = data.len();
        if count > 0 {
            self.parallelizer
                .sink_ddl(data.clone(), &self.sinkers)
                .await
                .unwrap();
            // only part of sinkers will execute sink_ddl, but all sinkers should refresh metadata
            for sinker in self.sinkers.iter_mut() {
                sinker
                    .lock()
                    .await
                    .refresh_meta(data.clone())
                    .await
                    .unwrap();
            }
        }
        Ok((count, last_received_position, last_commit_position))
    }

    #[inline(always)]
    fn record_checkpoint(
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
            log_position!("current_position | {}", position);
        }

        if let Some(position) = last_commit {
            log_position!("checkpoint_position | {}", position);
            self.syncer.lock().unwrap().checkpoint_position = position.clone();
        }

        log_monitor!("avg tps: {}", tps_counter.avg(),);
        log_monitor!("sinked count: {}", count_counter.value);

        Instant::now()
    }
}
