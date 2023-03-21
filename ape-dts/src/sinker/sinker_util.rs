use std::collections::HashMap;

use log::info;

use crate::{
    common::check_log_line::{self, CheckLogLine},
    meta::row_data::RowData,
};

pub struct SinkerUtil {}

const CHECK_MISS_FILE_LOGGER: &str = "check_miss_file_logger";
const CHECK_DIFF_FILE_LOGGER: &str = "check_diff_file_logger";

impl SinkerUtil {
    #[inline(always)]
    pub fn batch_compare_row_datas(
        src_data: &Vec<RowData>,
        dst_row_data_map: &HashMap<u128, RowData>,
        id_cols: &Vec<String>,
        start_index: usize,
        batch_size: usize,
    ) {
        for i in start_index..start_index + batch_size {
            let row_data_src = &src_data[i];
            let hash_code = row_data_src.get_hash_code(id_cols);
            if let Some(row_data_dst) = dst_row_data_map.get(&hash_code) {
                if !SinkerUtil::compare_row_data(row_data_src, row_data_dst) {
                    SinkerUtil::log_diff(&row_data_src, id_cols);
                }
            } else {
                SinkerUtil::log_miss(&row_data_src, id_cols);
            }
        }
    }

    #[inline(always)]
    pub fn compare_row_data(row_data_src: &RowData, row_data_dst: &RowData) -> bool {
        let src = row_data_src.after.as_ref().unwrap();
        let dst = row_data_dst.after.as_ref().unwrap();
        for (col, src_col_value) in src.iter() {
            if let Some(dst_col_value) = dst.get(col) {
                if src_col_value != dst_col_value {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    #[inline(always)]
    pub fn log_miss(row_data: &RowData, id_cols: &Vec<String>) {
        // TODO, batch
        let check_log_line = CheckLogLine::from_row_data(row_data, id_cols);
        info!(
            target: CHECK_MISS_FILE_LOGGER,
            "{}",
            check_log_line.to_string()
        );
    }

    #[inline(always)]
    pub fn log_diff(row_data: &RowData, id_cols: &Vec<String>) {
        let check_log_line = CheckLogLine::from_row_data(row_data, id_cols);
        info!(
            target: CHECK_DIFF_FILE_LOGGER,
            "{}",
            check_log_line.to_string()
        );
    }
}
