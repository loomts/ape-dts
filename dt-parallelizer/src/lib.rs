pub mod base_parallelizer;
pub mod check_parallelizer;
pub mod merge_parallelizer;
pub mod mongo_parallelizer;
pub mod partition_parallelizer;
pub mod rdb_merger;
pub mod rdb_partitioner;
pub mod serial_parallelizer;
pub mod snapshot_parallelizer;
pub mod table_parallelizer;

use std::sync::Arc;

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::error::Error;
use dt_connector::Sinker;
use dt_meta::{ddl_data::DdlData, dt_data::DtData, row_data::RowData};

#[async_trait]
pub trait Parallelizer {
    fn get_name(&self) -> String;

    async fn drain(&mut self, buffer: &ConcurrentQueue<DtData>) -> Result<Vec<DtData>, Error>;

    async fn sink_ddl(
        &mut self,
        data: Vec<DdlData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> Result<(), Error>;

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> Result<(), Error>;
}
