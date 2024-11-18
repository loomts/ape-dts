use std::collections::HashMap;

use anyhow::Ok;
use dt_common::utils::time_util::TimeUtil;

use super::{
    mongo_test_runner::DST,
    rdb_test_runner::{RdbTestRunner, SRC},
};

pub struct RdbStarrocksTestRunner {
    base: RdbTestRunner,
}

impl RdbStarrocksTestRunner {
    pub async fn new(relative_test_dir: &str) -> anyhow::Result<Self> {
        Ok(Self {
            base: RdbTestRunner::new(relative_test_dir).await?,
        })
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        self.base.close().await
    }

    pub async fn run_cdc_soft_delete_test(
        &self,
        start_millis: u64,
        parse_millis: u64,
    ) -> anyhow::Result<()> {
        // prepare src and dst tables
        self.base.execute_prepare_sqls().await?;

        // start task
        let task = self.base.spawn_cdc_task(start_millis, parse_millis).await?;

        // load dml sqls
        let (src_insert_sqls, src_update_sqls, src_delete_sqls) =
            RdbTestRunner::split_dml_sqls(&self.base.base.src_test_sqls);

        let (src_db_tbs, dst_db_tbs) = self.base.get_compare_db_tbs()?;

        // insert src data
        if !src_insert_sqls.is_empty() {
            self.base.execute_src_sqls(&src_insert_sqls).await?;
            TimeUtil::sleep_millis(parse_millis).await;
            self.base
                .compare_data_for_tbs(&src_db_tbs, &dst_db_tbs)
                .await?;
        }

        // update src data
        if !src_update_sqls.is_empty() {
            self.base.execute_src_sqls(&src_update_sqls).await?;
            TimeUtil::sleep_millis(parse_millis).await;
            self.base
                .compare_data_for_tbs(&src_db_tbs, &dst_db_tbs)
                .await?;
        }

        // get src tb row_count before delete
        let mut src_tb_row_count_map_before = HashMap::new();
        for db_tb in src_db_tbs.iter() {
            let src_data = self.base.fetch_data(db_tb, SRC).await?;
            src_tb_row_count_map_before.insert(db_tb.to_owned(), src_data.len());
        }

        // delete src data
        if !src_delete_sqls.is_empty() {
            self.base.execute_src_sqls(&src_delete_sqls).await?;
            TimeUtil::sleep_millis(parse_millis).await;
        }

        // get src tb row_count after delete
        let mut src_tb_row_count_map_after = HashMap::new();
        for db_tb in src_db_tbs.iter() {
            let src_data = self.base.fetch_data(db_tb, SRC).await?;
            src_tb_row_count_map_after.insert(db_tb.to_owned(), src_data.len());
        }

        // get dst tb row_count after soft delete
        let mut dst_tb_row_count_map = HashMap::new();
        let mut dst_tb_row_count_map_deleted = HashMap::new();

        for db_tb in dst_db_tbs.iter() {
            let dst_data = self.base.fetch_data(db_tb, DST).await?;
            dst_tb_row_count_map.insert(db_tb.to_owned(), dst_data.len());

            let condition = "WHERE _ape_dts_is_deleted != 1";
            let dst_data_deleted = self
                .base
                .fetch_data_with_condition(db_tb, DST, &condition)
                .await?;
            dst_tb_row_count_map_deleted.insert(db_tb.to_owned(), dst_data_deleted.len());
        }

        // compare after delete
        for i in 0..src_db_tbs.len() {
            let src_db_tb = &src_db_tbs[i];
            let dst_db_tb = &dst_db_tbs[i];

            assert_eq!(
                src_tb_row_count_map_before.get(src_db_tb).unwrap(),
                dst_tb_row_count_map.get(dst_db_tb).unwrap()
            );

            assert_eq!(
                src_tb_row_count_map_after.get(src_db_tb).unwrap(),
                dst_tb_row_count_map_deleted.get(dst_db_tb).unwrap()
            );
        }

        self.base.base.wait_task_finish(&task).await
    }
}
