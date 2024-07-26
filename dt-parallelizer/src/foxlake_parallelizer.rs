use std::sync::Arc;

use async_trait::async_trait;
use dt_common::{
    config::{sinker_config::SinkerConfig, task_config::TaskConfig},
    meta::{
        dt_data::{DtData, DtItem},
        dt_queue::DtQueue,
    },
    monitor::counter::Counter,
};
use dt_connector::Sinker;

use crate::{snapshot_parallelizer::SnapshotParallelizer, Parallelizer};

pub struct FoxlakeParallelizer {
    pub task_config: TaskConfig,
    pub base_parallelizer: SnapshotParallelizer,
}

#[async_trait]
impl Parallelizer for FoxlakeParallelizer {
    fn get_name(&self) -> String {
        "FoxlakeParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        match self.task_config.sinker {
            SinkerConfig::FoxlakeMerge { .. } => self.drain_foxlake(buffer).await,
            _ => self.base_parallelizer.drain(buffer).await,
        }
    }

    async fn sink_raw(
        &mut self,
        data: Vec<DtItem>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> anyhow::Result<()> {
        if matches!(self.task_config.sinker, SinkerConfig::FoxlakePush { .. }) {
            sinkers[0].lock().await.refresh_meta(Vec::new()).await?;
        }
        self.base_parallelizer.sink_raw(data, sinkers).await
    }
}

impl FoxlakeParallelizer {
    async fn drain_foxlake(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        let mut record_size_counter = Counter::new(0, 0);
        let base = &mut self.base_parallelizer.base_parallelizer;
        let mut data = Vec::new();

        let mut last_push_epoch = 0;
        // pop to find the push_epoch of the last item
        while let Ok(item) = base.pop(buffer, &mut record_size_counter).await {
            if let DtData::Foxlake { file_meta } = &item.dt_data {
                last_push_epoch = file_meta.push_epoch;
                base.poped_data.push_back(item);
            }
        }

        while let Some(item) = base.poped_data.front() {
            if let DtData::Foxlake { file_meta } = &item.dt_data {
                // to improve merge performance,
                // make sure all orc files of the same push_epoch be merged in the same batch.

                // the push_epoch of finished is i64::MAX, which ensures all items
                // in poped_data to be merged
                if file_meta.push_epoch < last_push_epoch {
                    data.push(base.poped_data.pop_front().unwrap())
                } else {
                    break;
                }
            }
        }

        base.update_monitor(&record_size_counter).await;
        Ok(data)
    }
}
