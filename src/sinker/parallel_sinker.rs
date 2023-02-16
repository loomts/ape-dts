use std::sync::atomic::{AtomicBool, Ordering};

use concurrent_queue::ConcurrentQueue;
use futures::future::join_all;
use log::{debug, info};

use crate::{error::Error, meta::row_data::RowData, task::task_util::TaskUtil};

use super::{slicer::Slicer, traits::Sinker};

pub struct ParallelSinker<'a> {
    pub buffer: &'a ConcurrentQueue<RowData>,
    pub slicer: Slicer,
    pub sub_sinkers: Vec<Box<dyn Sinker>>,
    pub shut_down: &'a AtomicBool,
}

const POSITION_FILE_LOGGER: &str = "position_file_logger";

impl ParallelSinker<'_> {
    pub async fn sink(&mut self) -> Result<(), Error> {
        while !self.shut_down.load(Ordering::Acquire) || !self.buffer.is_empty() {
            // process all row_datas in buffer at a time
            let slice_count = self.sub_sinkers.len();
            let mut all_data = Vec::new();
            while let Ok(row_data) = self.buffer.pop() {
                // if any col value of uk/pk changed, cut off the data and sink the pushed data immediately
                let (uk_col_changed, changed_col, col_value_before, col_value_after) =
                    self.slicer.check_uk_col_changed(&row_data).await?;
                if uk_col_changed {
                    debug!(
                        "{}.{}.{} changed from {} to {}",
                        &row_data.db,
                        &row_data.tb,
                        changed_col.unwrap(),
                        col_value_before.unwrap().to_string(),
                        col_value_after.unwrap().to_string()
                    );
                    all_data.push(row_data);
                    break;
                } else {
                    all_data.push(row_data);
                }
            }

            // record the last row_data for logging position_info
            let mut last_row_data = Option::None;
            if !all_data.is_empty() {
                last_row_data = Some(all_data[all_data.len() - 1].clone());
            }

            // slice data
            let mut sub_datas = self.slicer.slice(all_data, slice_count).await?;

            // start sub sinkers
            let mut futures = Vec::new();
            for sinker in self.sub_sinkers.iter_mut() {
                futures.push(sinker.sink(sub_datas.remove(0)));
            }

            // wait for sub sinkers to finish and unwrap errors if happen
            let result = join_all(futures).await;
            for res in result {
                res.unwrap();
            }

            // record position_info
            if let Some(row_data) = last_row_data {
                info!(target: POSITION_FILE_LOGGER, "{:?}", row_data.position_info);
            }

            // sleep 1 millis for data preparing
            TaskUtil::sleep_millis(1).await;
        }

        Ok(())
    }
}
