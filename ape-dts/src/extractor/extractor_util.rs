use concurrent_queue::ConcurrentQueue;

use crate::{error::Error, meta::row_data::RowData, task::task_util::TaskUtil};

pub struct ExtractorUtil {}

impl ExtractorUtil {
    pub async fn push_row(
        buffer: &ConcurrentQueue<RowData>,
        row_data: RowData,
    ) -> Result<(), Error> {
        while buffer.is_full() {
            TaskUtil::sleep_millis(1).await;
        }
        buffer.push(row_data).unwrap();
        Ok(())
    }
}
