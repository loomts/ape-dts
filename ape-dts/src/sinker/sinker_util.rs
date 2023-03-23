use std::collections::HashMap;

use log::info;

use crate::{
    common::check_log::CheckLog,
    error::Error,
    meta::{
        mysql::{mysql_meta_manager::MysqlMetaManager, mysql_tb_meta::MysqlTbMeta},
        pg::{pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
        rdb_tb_meta::RdbTbMeta,
        row_data::RowData,
    },
};

use super::rdb_router::RdbRouter;

pub struct SinkerUtil {}

const CHECK_MISS_FILE_LOGGER: &str = "check_miss_file_logger";
const CHECK_DIFF_FILE_LOGGER: &str = "check_diff_file_logger";

impl SinkerUtil {
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
                if !SinkerUtil::compare_row_data(row_data_src, row_data_dst) {
                    SinkerUtil::log_diff(&row_data_src, tb_meta);
                }
            } else {
                SinkerUtil::log_miss(&row_data_src, tb_meta);
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
        let check_log = CheckLog::from_row_data(row_data, tb_meta);
        info!(target: CHECK_MISS_FILE_LOGGER, "{}", check_log.to_string());
    }

    #[inline(always)]
    pub fn log_diff(row_data: &RowData, tb_meta: &RdbTbMeta) {
        let check_log = CheckLog::from_row_data(row_data, tb_meta);
        info!(target: CHECK_DIFF_FILE_LOGGER, "{}", check_log.to_string());
    }

    #[inline(always)]
    pub async fn get_mysql_tb_meta(
        meta_manager: &mut MysqlMetaManager,
        router: &mut RdbRouter,
        row_data: &RowData,
    ) -> Result<MysqlTbMeta, Error> {
        let (db, tb) = router.get_route(&row_data.db, &row_data.tb);
        let tb_meta = meta_manager.get_tb_meta(&db, &tb).await?;
        return Ok(tb_meta);
    }

    #[inline(always)]
    pub async fn get_pg_tb_meta(
        meta_manager: &mut PgMetaManager,
        router: &mut RdbRouter,
        row_data: &RowData,
    ) -> Result<PgTbMeta, Error> {
        let (db, tb) = router.get_route(&row_data.db, &row_data.tb);
        let tb_meta = meta_manager.get_tb_meta(&db, &tb).await?;
        return Ok(tb_meta);
    }

    // #[inline(always)]
    // pub fn check_result(
    //     actual_rows_affected: u64,
    //     expect_rows_affected: u64,
    //     sql: &str,
    //     row_data: &RowData,
    //     tb_meta: &RdbTbMeta,
    // ) -> Result<(), Error> {
    //     if actual_rows_affected != expect_rows_affected {
    //         info!(
    //             "sql: {}\nrows_affected: {},rows_affected_expected: {}\n{}",
    //             sql,
    //             actual_rows_affected,
    //             expect_rows_affected,
    //             row_data.to_string(tb_meta)
    //         );
    //     }
    //     Ok(())
    // }
}
