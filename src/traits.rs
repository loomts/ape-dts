use crate::{error::Error, meta::row_data::RowData};
use async_trait::async_trait;

#[async_trait]
pub trait Sinker {
    async fn sink(&mut self, mut data: Vec<RowData>) -> Result<(), Error>;

    async fn close(&mut self) -> Result<(), Error>;
}

pub trait Sinker2 {
    fn sink(&mut self, data: Vec<RowData>) -> Result<(), Error>;

    fn close(&mut self) -> Result<(), Error>;
}

#[async_trait]
pub trait Extractor {
    async fn extract(&mut self) -> Result<(), Error>;

    async fn close(&mut self) -> Result<(), Error>;
}

#[async_trait]
pub trait Partitioner {
    async fn partition(
        &mut self,
        data: Vec<RowData>,
        slice_count: usize,
    ) -> Result<Vec<Vec<RowData>>, Error>;

    async fn can_be_partitioned<'a>(&mut self, row_data: &'a RowData) -> Result<bool, Error>;
}
