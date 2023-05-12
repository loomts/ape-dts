pub mod extractor;
pub mod sinker;

use async_trait::async_trait;
use dt_common::{
    error::Error,
    log::check_log::CheckLog,
    meta::{ddl_data::DdlData, row_data::RowData},
};

#[async_trait]
pub trait Sinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> Result<(), Error>;

    async fn sink_ddl(&mut self, mut data: Vec<DdlData>, batch: bool) -> Result<(), Error>;

    async fn close(&mut self) -> Result<(), Error>;
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
