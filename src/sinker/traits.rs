use crate::{error::Error, meta::row_data::RowData};
use async_trait::async_trait;

#[async_trait]
pub trait Sinker {
    async fn sink(&mut self, mut data: Vec<RowData>) -> Result<(), Error>;
}
