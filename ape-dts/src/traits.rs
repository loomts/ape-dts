use std::sync::Arc;

use crate::{
    error::Error,
    log::check_log::CheckLog,
    meta::{ddl_data::DdlData, dt_data::DtData, row_data::RowData},
};
use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;

#[async_trait]
pub trait Sinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> Result<(), Error>;

    async fn sink_ddl(&mut self, mut data: Vec<DdlData>, batch: bool) -> Result<(), Error>;

    async fn close(&mut self) -> Result<(), Error>;
}

#[async_trait]
pub trait Parallelizer {
    fn get_name(&self) -> String;

    async fn drain(&mut self, buffer: &ConcurrentQueue<DtData>) -> Result<Vec<DtData>, Error>;

    async fn sink_ddl(
        &mut self,
        data: Vec<DdlData>,
        sinkers: &Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    ) -> Result<(), Error>;

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    ) -> Result<(), Error>;
}

#[async_trait]
pub trait Extractor {
    async fn extract(&mut self) -> Result<(), Error>;

    async fn close(&mut self) -> Result<(), Error>;
}

#[async_trait]
pub trait BatchCheckExtractor {
    async fn batch_extract(&mut self, check_logs: &Vec<CheckLog>) -> Result<(), Error>;
}
