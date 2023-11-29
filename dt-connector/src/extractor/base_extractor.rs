use std::{
    collections::HashMap,
    sync::atomic::{AtomicBool, Ordering},
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

pub struct BaseExtractor {}

impl BaseExtractor {
    pub async fn push_dt_data(
        buffer: &ConcurrentQueue<DtItem>,
        dt_data: DtData,
        position: Position,
    ) -> Result<(), Error> {
        while buffer.is_full() {
            TimeUtil::sleep_millis(1).await;
        }
        let item = DtItem { dt_data, position };
        buffer.push(item).unwrap();
        Ok(())
    }

    pub async fn push_row(
        buffer: &ConcurrentQueue<DtItem>,
        row_data: RowData,
        position: Position,
        router: Option<&RdbRouter>,
    ) -> Result<(), Error> {
        let row_data = Self::route_row(row_data, router);
        let dt_data = DtData::Dml { row_data };
        Self::push_dt_data(buffer, dt_data, position).await
    }

    fn route_row(mut row_data: RowData, router: Option<&RdbRouter>) -> RowData {
        if router.is_none() {
            return row_data;
        }
        let router = router.unwrap();

        // tb map
        let (schema, tb) = (row_data.schema.clone(), row_data.tb.clone());
        let (dst_schema, dst_tb) = router.get_tb_map(&schema, &tb);
        row_data.schema = dst_schema.to_string();
        row_data.tb = dst_tb.to_string();

        // col map
        let col_map = router.get_col_map(&schema, &tb);
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

    pub async fn wait_task_finish(
        buffer: &ConcurrentQueue<DtItem>,
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
