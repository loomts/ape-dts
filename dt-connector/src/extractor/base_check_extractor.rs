use std::sync::atomic::AtomicBool;

use concurrent_queue::ConcurrentQueue;
use dt_common::log::check_log::CheckLog;

use dt_common::{error::Error, log::log_reader::LogReader, meta::dt_data::DtData};

use crate::BatchCheckExtractor;

use super::base_extractor::BaseExtractor;

pub struct BaseCheckExtractor<'a> {
    pub check_log_dir: String,
    pub buffer: &'a ConcurrentQueue<DtData>,
    pub batch_size: usize,
    pub shut_down: &'a AtomicBool,
}

impl BaseCheckExtractor<'_> {
    pub async fn extract(
        &mut self,
        extractor: &mut Box<&mut (dyn BatchCheckExtractor + Send)>,
    ) -> Result<(), Error> {
        let mut log_reader = LogReader::new(&self.check_log_dir);
        let mut batch = Vec::new();

        while let Some(log) = log_reader.next() {
            if log.trim().is_empty() {
                continue;
            }
            let check_log = CheckLog::from_str(&log, log_reader.log_type.clone());

            if Self::can_in_same_batch(&batch, &check_log) {
                batch.push(check_log);
            } else {
                Self::batch_extract_and_clear(extractor, &mut batch).await;
                batch.push(check_log);
            }

            if batch.len() >= self.batch_size
                || (batch.len() == 1 && Self::is_any_col_none(&batch[0]))
            {
                Self::batch_extract_and_clear(extractor, &mut batch).await;
            }
        }

        Self::batch_extract_and_clear(extractor, &mut batch).await;
        BaseExtractor::wait_task_finish(self.buffer, self.shut_down).await
    }

    async fn batch_extract_and_clear(
        extractor: &mut Box<&mut (dyn BatchCheckExtractor + Send)>,
        batch: &mut Vec<CheckLog>,
    ) {
        extractor.batch_extract(&batch).await.unwrap();
        batch.clear();
    }

    fn can_in_same_batch(exist_items: &Vec<CheckLog>, new_item: &CheckLog) -> bool {
        if exist_items.is_empty() {
            return true;
        }

        let same_tb = exist_items[0].schema == new_item.schema && exist_items[0].tb == new_item.tb;
        let same_log_type = exist_items[0].log_type == new_item.log_type;
        let any_col_none = Self::is_any_col_none(new_item);
        same_tb && same_log_type && !any_col_none
    }

    fn is_any_col_none(check_log: &CheckLog) -> bool {
        for i in check_log.col_values.iter() {
            if i.is_none() {
                return true;
            }
        }
        false
    }
}
