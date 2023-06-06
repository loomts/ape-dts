use std::collections::HashMap;

use dt_common::{log_diff, log_miss};
use dt_meta::{rdb_tb_meta::RdbTbMeta, row_data::RowData};

use crate::check_log::{check_log::CheckLog, log_type::LogType};

pub struct BaseChecker {}

impl BaseChecker {
    #[inline(always)]
    pub fn batch_compare_row_datas(
        src_data: &Vec<RowData>,
        dst_row_data_map: &HashMap<u128, RowData>,
        tb_meta: &RdbTbMeta,
        start_index: usize,
        batch_size: usize,
    ) {
        for i in start_index..start_index + batch_size {
            let row_data_src = &src_data[i];
            let hash_code = row_data_src.get_hash_code(tb_meta);
            if let Some(row_data_dst) = dst_row_data_map.get(&hash_code) {
                if !Self::compare_row_data(row_data_src, row_data_dst) {
                    Self::log_diff(&row_data_src, tb_meta);
                }
            } else {
                Self::log_miss(&row_data_src, tb_meta);
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
    pub fn log_miss(row_data: &RowData, tb_meta: &RdbTbMeta) {
        // TODO, batch write
        let check_log = CheckLog::from_row_data(row_data, tb_meta, LogType::Miss);
        log_miss!("{}", check_log.to_string());
    }

    #[inline(always)]
    pub fn log_diff(row_data: &RowData, tb_meta: &RdbTbMeta) {
        let check_log = CheckLog::from_row_data(row_data, tb_meta, LogType::Diff);
        log_diff!("{}", check_log.to_string());
    }
}
