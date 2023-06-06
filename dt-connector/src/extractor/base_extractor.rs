use std::sync::atomic::{AtomicBool, Ordering};

use concurrent_queue::ConcurrentQueue;

use dt_common::{error::Error, utils::time_util::TimeUtil};
use dt_meta::{dt_data::DtData, row_data::RowData};

pub struct BaseExtractor {}

impl BaseExtractor {
    pub async fn push_dt_data(
        buffer: &ConcurrentQueue<DtData>,
        dt_data: DtData,
    ) -> Result<(), Error> {
        while buffer.is_full() {
            TimeUtil::sleep_millis(1).await;
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

    pub async fn wait_task_finish(
        buffer: &ConcurrentQueue<DtData>,
        shut_down: &AtomicBool,
    ) -> Result<(), Error> {
        // wait all data to be transfered
        while !buffer.is_empty() {
            TimeUtil::sleep_millis(1).await;
        }

        shut_down.store(true, Ordering::Release);
        Ok(())
    }
}
