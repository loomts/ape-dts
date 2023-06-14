pub mod check_log;
pub mod extractor;
pub mod rdb_query_builder;
pub mod sinker;

use async_trait::async_trait;
use check_log::check_log::CheckLog;
use dt_common::error::Error;
use dt_meta::{ddl_data::DdlData, row_data::RowData};

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
