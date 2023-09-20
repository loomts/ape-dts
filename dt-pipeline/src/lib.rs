pub mod base_parallelizer;
pub mod check_parallelizer;
pub mod merge_parallelizer;
pub mod mongo_parallelizer;
pub mod partition_parallelizer;
pub mod pipeline;
pub mod rdb_merger;
pub mod rdb_partitioner;
pub mod serial_parallelizer;
pub mod snapshot_parallelizer;
pub mod table_parallelizer;

// new:
// pub mod base_pipeline;
// pub mod filters;
// pub mod transaction_pipeline;
// pub mod utils;

use std::sync::Arc;

use async_trait::async_trait;
use dt_common::error::Error;
use dt_connector::Sinker;
use dt_meta::{ddl_data::DdlData, dt_data::DtData, row_data::RowData};
use merge_parallelizer::TbMergedData;

#[async_trait]
pub trait Pipeline {
    async fn start(&mut self) -> Result<(), Error>;

    async fn stop(&mut self) -> Result<(), Error>;

    // merge methods:

    async fn drain(&mut self, buffer: &ConcurrentQueue<DtData>) -> Result<Vec<DtData>, Error> {
        Ok(Vec::new())
    }

    async fn sink_ddl(
        &mut self,
        data: Vec<DdlData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> Result<(), Error> {
        Ok(())
    }
}
