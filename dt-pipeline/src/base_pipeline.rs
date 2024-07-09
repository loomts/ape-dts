use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    time::Instant,
};

use async_trait::async_trait;
use dt_common::{
    config::sinker_config::SinkerConfig,
    meta::{
        ddl_data::DdlData,
        dt_data::{DtData, DtItem},
        dt_queue::DtQueue,
        position::Position,
        row_data::RowData,
        syncer::Syncer,
    },
};
use dt_common::{
    log_info, log_position,
    monitor::{counter_type::CounterType, monitor::Monitor},
    utils::time_util::TimeUtil,
};
use dt_connector::{data_marker::DataMarker, Sinker};
use dt_parallelizer::Parallelizer;

use crate::{lua_processor::LuaProcessor, Pipeline};

pub struct BasePipeline {
    pub buffer: Arc<DtQueue>,
    pub parallelizer: Box<dyn Parallelizer + Send>,
    pub sinker_config: SinkerConfig,
    pub sinkers: Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    pub shut_down: Arc<AtomicBool>,
    pub checkpoint_interval_secs: u64,
    pub batch_sink_interval_secs: u64,
    pub syncer: Arc<Mutex<Syncer>>,
    pub monitor: Arc<Mutex<Monitor>>,
    pub data_marker: Option<Arc<RwLock<DataMarker>>>,
    pub lua_processor: Option<LuaProcessor>,
}

enum SinkMethod {
    Raw,
    Ddl,
    Dml,
}

#[async_trait]
impl Pipeline for BasePipeline {
    async fn stop(&mut self) -> anyhow::Result<()> {
        for sinker in self.sinkers.iter_mut() {
            sinker.lock().await.close().await?;
        }
        self.parallelizer.close().await
    }

    async fn start(&mut self) -> anyhow::Result<()> {
        log_info!(
            "{} starts, parallel_size: {}, checkpoint_interval_secs: {}",
            self.parallelizer.get_name(),
            self.sinkers.len(),
            self.checkpoint_interval_secs
        );

        let mut last_sink_time = Instant::now();
        let mut last_checkpoint_time = Instant::now();
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;

        while !self.shut_down.load(Ordering::Acquire) || !self.buffer.is_empty() {
            // to avoid too many sub counters, only add counter when buffer is not empty
            if !self.buffer.is_empty() {
                self.monitor
                    .lock()
                    .unwrap()
                    .add_counter(CounterType::BufferSize, self.buffer.len());
            }

            // some sinkers (foxlake) need to accumulate data to a big batch and sink
            let data = if last_sink_time.elapsed().as_secs() < self.batch_sink_interval_secs
                && !self.buffer.is_full()
            {
                Vec::new()
            } else {
                last_sink_time = Instant::now();
                self.parallelizer.drain(self.buffer.as_ref()).await?
            };

            if let Some(data_marker) = &mut self.data_marker {
                if !data.is_empty() {
                    data_marker.write().unwrap().data_origin_node =
                        data[0].data_origin_node.clone();
                }
            }

            // process all row_datas in buffer at a time
            let (count, last_received, last_commit) = match self.get_sink_method(&data) {
                SinkMethod::Ddl => self.sink_ddl(data).await?,
                SinkMethod::Dml => self.sink_dml(data).await?,
                SinkMethod::Raw => self.sink_raw(data).await?,
            };

            if let Some(position) = &last_received {
                self.syncer.lock().unwrap().received_position = position.to_owned();
                last_received_position = last_received;
            }
            if last_commit.is_some() {
                last_commit_position = last_commit;
            }

            last_checkpoint_time = self.record_checkpoint(
                Some(last_checkpoint_time),
                &last_received_position,
                &last_commit_position,
            );

            self.monitor
                .lock()
                .unwrap()
                .add_counter(CounterType::SinkedCount, count);

            // sleep 1 millis for data preparing
            TimeUtil::sleep_millis(1).await;
        }

        self.record_checkpoint(None, &last_received_position, &last_commit_position);
        Ok(())
    }
}

impl BasePipeline {
    async fn sink_raw(
        &mut self,
        all_data: Vec<DtItem>,
    ) -> anyhow::Result<(usize, Option<Position>, Option<Position>)> {
        let (last_received_position, last_commit_position) = Self::fetch_raw(&all_data);
        let count = all_data.len();
        if count > 0 {
            self.parallelizer.sink_raw(all_data, &self.sinkers).await?
        }
        Ok((count, last_received_position, last_commit_position))
    }

    async fn sink_dml(
        &mut self,
        all_data: Vec<DtItem>,
    ) -> anyhow::Result<(usize, Option<Position>, Option<Position>)> {
        let (mut data, last_received_position, last_commit_position) = Self::fetch_dml(all_data);
        let count = data.len();
        if count > 0 {
            // execute lua processor
            if let Some(lua_processor) = &self.lua_processor {
                data = lua_processor.process(data)?;
            }

            self.parallelizer.sink_dml(data, &self.sinkers).await?
        }
        Ok((count, last_received_position, last_commit_position))
    }

    async fn sink_ddl(
        &mut self,
        all_data: Vec<DtItem>,
    ) -> anyhow::Result<(usize, Option<Position>, Option<Position>)> {
        let (data, last_received_position, last_commit_position) = Self::fetch_ddl(all_data);
        let count = data.len();
        if count > 0 {
            self.parallelizer
                .sink_ddl(data.clone(), &self.sinkers)
                .await?;
            // only part of sinkers will execute sink_ddl, but all sinkers should refresh metadata
            for sinker in self.sinkers.iter_mut() {
                sinker.lock().await.refresh_meta(data.clone()).await?;
            }
        }
        Ok((count, last_received_position, last_commit_position))
    }

    fn fetch_raw(data: &[DtItem]) -> (Option<Position>, Option<Position>) {
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;
        for i in data.iter() {
            match &i.dt_data {
                DtData::Commit { .. } => {
                    last_commit_position = Some(i.position.clone());
                    last_received_position = last_commit_position.clone();
                    continue;
                }

                DtData::Redis { entry } => {
                    last_received_position = Some(i.position.clone());
                    last_commit_position = last_received_position.clone();
                    if !entry.is_raw() && entry.cmd.get_name().eq_ignore_ascii_case("ping") {
                        continue;
                    }
                }

                DtData::Begin {} => {
                    continue;
                }

                _ => {
                    last_received_position = Some(i.position.clone());
                }
            }
        }

        (last_received_position, last_commit_position)
    }

    fn fetch_dml(mut data: Vec<DtItem>) -> (Vec<RowData>, Option<Position>, Option<Position>) {
        let mut dml_data = Vec::new();
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;
        for i in data.drain(..) {
            match i.dt_data {
                DtData::Commit { .. } => {
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                    continue;
                }

                DtData::Dml { row_data } => {
                    last_received_position = Some(i.position);
                    dml_data.push(row_data);
                }

                _ => {}
            }
        }

        (dml_data, last_received_position, last_commit_position)
    }

    fn fetch_ddl(mut data: Vec<DtItem>) -> (Vec<DdlData>, Option<Position>, Option<Position>) {
        let mut result = Vec::new();
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;
        for i in data.drain(..) {
            match i.dt_data {
                DtData::Commit { .. } => {
                    last_commit_position = Some(i.position);
                    last_received_position = last_commit_position.clone();
                    continue;
                }

                DtData::Ddl { ddl_data } => {
                    last_received_position = Some(i.position);
                    result.push(ddl_data);
                }

                _ => {}
            }
        }

        (result, last_received_position, last_commit_position)
    }

    fn get_sink_method(&self, data: &Vec<DtItem>) -> SinkMethod {
        match self.sinker_config {
            SinkerConfig::FoxlakePush { .. }
            | SinkerConfig::FoxlakeMerge { .. }
            | SinkerConfig::Foxlake { .. } => return SinkMethod::Raw,
            _ => {}
        }

        for i in data {
            match i.dt_data {
                DtData::Ddl { .. } => return SinkMethod::Ddl,
                DtData::Dml { .. } => return SinkMethod::Dml,
                DtData::Redis { .. } | DtData::Foxlake { .. } => return SinkMethod::Raw,
                DtData::Begin {} | DtData::Commit { .. } => {
                    continue;
                }
            }
        }
        SinkMethod::Raw
    }

    fn record_checkpoint(
        &self,
        last_checkpoint_time: Option<Instant>,
        last_received_position: &Option<Position>,
        last_commit_position: &Option<Position>,
    ) -> Instant {
        if let Some(last) = last_checkpoint_time {
            if last.elapsed().as_secs() < self.checkpoint_interval_secs {
                return last;
            }
        }

        if let Some(position) = last_received_position {
            log_position!("current_position | {}", position.to_string());
        }

        if let Some(position) = last_commit_position {
            log_position!("checkpoint_position | {}", position.to_string());
            self.syncer.lock().unwrap().committed_position = position.clone();
        }
        Instant::now()
    }
}
