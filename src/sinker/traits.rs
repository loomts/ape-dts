use crate::{error::Error, meta::row_data::RowData};
use async_trait::async_trait;

#[async_trait]
pub trait Sinker {
    async fn sink(&mut self) -> Result<(), Error>;

    async fn accept(&self, row_data: RowData) -> Result<(), Error>;
}
