use std::sync::Arc;

use crate::{error::Error, meta::row_data::RowData};
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

    async fn drain(&mut self, buffer: &ConcurrentQueue<RowData>) -> Result<Vec<RowData>, Error>;

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
