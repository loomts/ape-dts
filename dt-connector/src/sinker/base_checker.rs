use std::collections::HashMap;

use dt_common::{error::Error, log_diff, log_extra, log_miss, utils::rdb_filter::RdbFilter};
use dt_meta::{
    rdb_meta_manager::RdbMetaManager, rdb_tb_meta::RdbTbMeta, row_data::RowData,
    struct_meta::statement::struct_statement::StructStatement,
};

use crate::{
    check_log::{check_log::CheckLog, log_type::LogType},
    rdb_router::RdbRouter,
};

pub struct BaseChecker {}

impl BaseChecker {
    #[inline(always)]
    pub fn batch_compare_row_datas(
        src_data: &[RowData],
        dst_row_data_map: &HashMap<u128, RowData>,
        tb_meta: &RdbTbMeta,
        start_index: usize,
        batch_size: usize,
    ) -> (Vec<RowData>, Vec<RowData>) {
        let mut miss = Vec::new();
        let mut diff = Vec::new();
        for row_data_src in src_data.iter().skip(start_index).take(batch_size) {
            let hash_code = row_data_src.get_hash_code(tb_meta);
            if let Some(row_data_dst) = dst_row_data_map.get(&hash_code) {
                if !Self::compare_row_data(row_data_src, row_data_dst) {
                    diff.push(row_data_src.to_owned());
                }
            } else {
                miss.push(row_data_src.to_owned());
            }
        }
        (miss, diff)
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

    pub async fn log_dml(
        extractor_meta_manager: &mut RdbMetaManager,
        router: &RdbRouter,
        miss: Vec<RowData>,
        diff: Vec<RowData>,
    ) -> Result<(), Error> {
        for row_data in miss {
            let src_row_data = router.route_row(row_data);
            let tb_meta = extractor_meta_manager
                .get_tb_meta(&src_row_data.schema, &src_row_data.tb)
                .await?;
            let check_log = CheckLog::from_row_data(&src_row_data, &tb_meta, LogType::Miss);
            log_miss!("{}", check_log.to_string());
        }

        for row_data in diff {
            let src_row_data = router.route_row(row_data);
            let tb_meta = extractor_meta_manager
                .get_tb_meta(&src_row_data.schema, &src_row_data.tb)
                .await?;
            let check_log = CheckLog::from_row_data(&src_row_data, &tb_meta, LogType::Miss);
            log_diff!("{}", check_log.to_string());
        }
        Ok(())
    }

    pub async fn log_mongo_dml(
        tb_meta: &RdbTbMeta,
        router: &RdbRouter,
        miss: Vec<RowData>,
        diff: Vec<RowData>,
    ) -> Result<(), Error> {
        for row_data in miss {
            let src_row_data = router.route_row(row_data);
            let check_log = CheckLog::from_row_data(&src_row_data, &tb_meta, LogType::Miss);
            log_miss!("{}", check_log.to_string());
        }

        for row_data in diff {
            let src_row_data = router.route_row(row_data);
            let check_log = CheckLog::from_row_data(&src_row_data, &tb_meta, LogType::Miss);
            log_diff!("{}", check_log.to_string());
        }
        Ok(())
    }

    #[inline(always)]
    pub fn compare_struct(
        src_statement: &mut StructStatement,
        dst_statement: &mut Option<StructStatement>,
        filter: &RdbFilter,
    ) -> Result<(), Error> {
        if dst_statement.is_none() {
            log_miss!("{:?}", src_statement.to_sqls(filter));
            return Ok(());
        }

        let mut src_sqls = HashMap::new();
        for (key, sql) in src_statement.to_sqls(filter) {
            src_sqls.insert(key, sql);
        }

        let mut dst_sqls = HashMap::new();
        for (key, sql) in dst_statement.as_mut().unwrap().to_sqls(filter) {
            dst_sqls.insert(key, sql);
        }

        for (key, src_sql) in src_sqls.iter() {
            if let Some(dst_sql) = dst_sqls.get(key) {
                if src_sql != dst_sql {
                    log_diff!("key: {}, src_sql: {}", key, src_sql);
                    log_diff!("key: {}, dst_sql: {}", key, dst_sql);
                }
            } else {
                log_miss!("key: {}, src_sql: {}", key, src_sql);
            }
        }

        for (key, dst_sql) in dst_sqls.iter() {
            if !src_sqls.contains_key(key) {
                log_extra!("key: {}, dst_sql: {}", key, dst_sql);
            }
        }

        Ok(())
    }
}
