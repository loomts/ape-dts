use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use dt_common::error::Error;
use dt_common::log_statistic;
use dt_common::monitor::monitor::Monitor;
use dt_meta::dt_data::DtData;
use serde::Serialize;
use serde_json::json;

use crate::Sinker;

pub struct RedisStatisticSinker {
    pub monitor: Arc<Mutex<Monitor>>,
    pub data_size_threshold: usize,
}

#[derive(Serialize)]
struct StatisticInfo {
    db_id: i64,
    key: String,
    data_size: usize,
}

#[async_trait]
impl Sinker for RedisStatisticSinker {
    async fn sink_raw(&mut self, mut data: Vec<DtData>, _batch: bool) -> Result<(), Error> {
        for dt_data in data.iter_mut() {
            if let DtData::Redis { entry } = dt_data {
                if entry.get_data_malloc_size() >= self.data_size_threshold {
                    let info = StatisticInfo {
                        db_id: entry.db_id,
                        key: entry.key.to_string(),
                        data_size: entry.get_data_malloc_size(),
                    };
                    log_statistic!("{}", json!(info).to_string());
                }
            }
        }
        Ok(())
    }
}
