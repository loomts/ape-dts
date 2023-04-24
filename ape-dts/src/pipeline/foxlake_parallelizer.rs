use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;

use crate::{
    error::Error,
    meta::{ddl_data::DdlData, dt_data::DtData, row_data::RowData},
    traits::{Parallelizer, Sinker},
};

use super::base_parallelizer::BaseParallelizer;

pub struct FoxlakeParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub parallel_size: usize,
}

#[async_trait]
impl Parallelizer for FoxlakeParallelizer {
    fn get_name(&self) -> String {
        "FoxlakeParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &ConcurrentQueue<DtData>) -> Result<Vec<DtData>, Error> {
        self.base_parallelizer.drain(buffer)
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    ) -> Result<(), Error> {
        let sub_datas = Self::partition(data)?;
        self.base_parallelizer
            .sink_dml(sub_datas, sinkers, 1, false)
            .await
    }

    async fn sink_ddl(
        &mut self,
        data: Vec<DdlData>,
        sinkers: &Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    ) -> Result<(), Error> {
        self.base_parallelizer
            .sink_ddl(vec![data], sinkers, 1, false)
            .await
    }
}

impl FoxlakeParallelizer {
    pub fn partition(data: Vec<RowData>) -> Result<Vec<Vec<RowData>>, Error> {
        let mut sub_data_map: HashMap<String, Vec<RowData>> = HashMap::new();
        for row_data in data {
            let full_tb = format!("{}.{}", row_data.db, row_data.tb);
            if let Some(sub_data) = sub_data_map.get_mut(&full_tb) {
                sub_data.push(row_data);
            } else {
                let sub_data = vec![row_data];
                sub_data_map.insert(full_tb, sub_data);
            }
        }

        let sub_datas = sub_data_map.into_iter().map(|(_, v)| v).collect();
        Ok(sub_datas)
    }
}
