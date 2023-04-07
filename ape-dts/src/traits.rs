use std::sync::Arc;

use crate::{
    error::Error,
    log::check_log::CheckLog,
    meta::{dt_data::DtData, row_data::RowData},
};
use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;

#[async_trait]
pub trait Sinker {
    async fn sink(&mut self, mut data: Vec<RowData>) -> Result<(), Error>;

    async fn batch_sink(&mut self, data: Vec<RowData>) -> Result<(), Error>;

    async fn close(&mut self) -> Result<(), Error>;
}

#[async_trait]
pub trait Parallelizer {
    fn get_name(&self) -> String;

    async fn drain(&mut self, buffer: &ConcurrentQueue<DtData>) -> Result<Vec<DtData>, Error>;

    async fn sink(
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
