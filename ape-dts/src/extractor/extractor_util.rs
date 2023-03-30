use concurrent_queue::ConcurrentQueue;

use crate::{
    error::Error,
    meta::{dt_data::DtData, row_data::RowData},
    task::task_util::TaskUtil,
};

pub struct ExtractorUtil {}

impl ExtractorUtil {
    pub async fn push_dt_data(
        buffer: &ConcurrentQueue<DtData>,
        dt_data: DtData,
    ) -> Result<(), Error> {
        while buffer.is_full() {
            TaskUtil::sleep_millis(1).await;
        }
        buffer.push(dt_data).unwrap();
        Ok(())
    }

    pub async fn push_row(
        buffer: &ConcurrentQueue<DtData>,
        row_data: RowData,
    ) -> Result<(), Error> {
        let dt_data = DtData::Dml { row_data };
        Self::push_dt_data(buffer, dt_data).await
    }
}
