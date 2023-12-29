use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use concurrent_queue::ConcurrentQueue;

use dt_common::{error::Error, utils::time_util::TimeUtil};
use dt_meta::{
    col_value::ColValue,
    dt_data::{DtData, DtItem},
    position::Position,
    row_data::RowData,
};

use crate::rdb_router::RdbRouter;

use super::extractor_monitor::ExtractorMonitor;

pub struct BaseExtractor {
    pub buffer: Arc<ConcurrentQueue<DtItem>>,
    pub router: RdbRouter,
    pub shut_down: Arc<AtomicBool>,
    pub monitor: ExtractorMonitor,
}

impl BaseExtractor {
    pub async fn push_dt_data(&mut self, dt_data: DtData, position: Position) -> Result<(), Error> {
        while self.buffer.is_full() {
            TimeUtil::sleep_millis(1).await;
        }

        self.monitor.counters.record_count += 1;
        self.monitor.counters.data_size += dt_data.get_data_size();
        self.monitor.try_flush(false);

        let item = DtItem { dt_data, position };
        self.buffer.push(item).unwrap();
        Ok(())
    }

    pub async fn push_row(&mut self, row_data: RowData, position: Position) -> Result<(), Error> {
        let row_data = self.route_row(row_data);
        let dt_data = DtData::Dml { row_data };
        self.push_dt_data(dt_data, position).await
    }

    fn route_row(&self, mut row_data: RowData) -> RowData {
        // tb map
        let (schema, tb) = (row_data.schema.clone(), row_data.tb.clone());
        let (dst_schema, dst_tb) = self.router.get_tb_map(&schema, &tb);
        row_data.schema = dst_schema.to_string();
        row_data.tb = dst_tb.to_string();

        // col map
        let col_map = self.router.get_col_map(&schema, &tb);
        if col_map.is_none() {
            return row_data;
        }
        let col_map = col_map.unwrap();

        let route_col_values =
            |col_values: HashMap<String, ColValue>| -> HashMap<String, ColValue> {
                let mut new_col_values = HashMap::new();
                for (col, col_value) in col_values {
                    if let Some(dst_col) = col_map.get(&col) {
                        new_col_values.insert(dst_col.to_owned(), col_value);
                    } else {
                        new_col_values.insert(col, col_value);
                    }
                }
                new_col_values
            };

        if let Some(before) = row_data.before {
            row_data.before = Some(route_col_values(before));
        }

        if let Some(after) = row_data.after {
            row_data.after = Some(route_col_values(after));
        }

        return row_data;
    }

    pub async fn wait_task_finish(&mut self) -> Result<(), Error> {
        // wait all data to be transfered
        while !self.buffer.is_empty() {
            TimeUtil::sleep_millis(1).await;
        }

        self.monitor.try_flush(true);
        self.shut_down.store(true, Ordering::Release);
        Ok(())
    }
}
