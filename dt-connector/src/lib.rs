pub mod check_log;
pub mod datamarker;
pub mod extractor;
pub mod meta_fetcher;
pub mod rdb_query_builder;
pub mod sinker;

use async_trait::async_trait;
use check_log::check_log::CheckLog;
use dt_common::error::Error;
use dt_meta::{ddl_data::DdlData, dt_data::DtData, row_data::RowData};

#[async_trait]
pub trait Sinker {
    async fn sink_dml(&mut self, mut _data: Vec<RowData>, _batch: bool) -> Result<(), Error> {
        Ok(())
    }

    async fn sink_ddl(&mut self, mut _data: Vec<DdlData>, _batch: bool) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn sink_raw(&mut self, mut _data: Vec<DtData>, _batch: bool) -> Result<(), Error> {
        Ok(())
    }

    async fn refresh_meta(&mut self, _data: Vec<DdlData>) -> Result<(), Error> {
        Ok(())
    }
}

#[async_trait]
pub trait Extractor {
    async fn extract(&mut self) -> Result<(), Error>;

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[async_trait]
pub trait BatchCheckExtractor {
    async fn batch_extract(&mut self, check_logs: &[CheckLog]) -> Result<(), Error>;
}
