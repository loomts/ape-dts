use std::time::{SystemTime, UNIX_EPOCH};

use log::info;

use crate::{error::Error, meta::row_data::RowData};

pub struct Logger {
    pub last_position_log_time: u128,
}

const LOG_POSITION_INTERVAL_MILLIS: u128 = 1000;
const POSITION_FILE_LOGGER: &str = "position_file_logger";

impl Logger {
    pub fn new() -> Self {
        Self {
            last_position_log_time: Self::get_system_time(),
        }
    }

    pub fn log_position(&mut self, row_data: &Option<RowData>, force: bool) -> Result<(), Error> {
        if row_data.is_none() {
            return Ok(());
        }

        let cur_time = Self::get_system_time();
        if force || cur_time > self.last_position_log_time + LOG_POSITION_INTERVAL_MILLIS {
            let position_info = &(row_data.as_ref().unwrap().position_info);
            if position_info.is_none() {
                return Ok(());
            }

            info!(
                target: POSITION_FILE_LOGGER,
                "{:?}",
                position_info.as_ref().unwrap()
            );
            self.last_position_log_time = cur_time;
        }

        Ok(())
    }

    fn get_system_time() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    }
}
