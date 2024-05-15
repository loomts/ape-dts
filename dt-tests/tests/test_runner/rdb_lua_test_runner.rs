use dt_common::utils::time_util::TimeUtil;

use super::rdb_test_runner::RdbTestRunner;

pub struct RdbLuaTestRunner {
    src_to_dst_runner: RdbTestRunner,
    expect_to_dst_runner: RdbTestRunner,
}

impl RdbLuaTestRunner {
    pub async fn new(relative_test_dir: &str) -> anyhow::Result<Self> {
        let src_to_dst_runner =
            RdbTestRunner::new_default(&format!("{}/src_to_dst", relative_test_dir)).await?;
        let expect_to_dst_runner =
            RdbTestRunner::new(&format!("{}/expect_to_dst", relative_test_dir), false).await?;
        Ok(Self {
            src_to_dst_runner,
            expect_to_dst_runner,
        })
    }

    pub async fn close(&self) -> anyhow::Result<()> {
        self.expect_to_dst_runner.close().await?;
        self.src_to_dst_runner.close().await
    }

    pub async fn run_snapshot_test(&self) -> anyhow::Result<()> {
        self.src_to_dst_runner.execute_prepare_sqls().await?;
        self.expect_to_dst_runner.execute_prepare_sqls().await?;

        self.src_to_dst_runner.execute_test_sqls().await?;
        self.expect_to_dst_runner.execute_test_sqls().await?;

        // start task
        self.src_to_dst_runner.base.start_task().await?;

        // compare data
        let (expect_db_tbs, dst_db_tbs) = self.expect_to_dst_runner.get_compare_db_tbs()?;
        self.expect_to_dst_runner
            .compare_data_for_tbs(&expect_db_tbs, &dst_db_tbs)
            .await?;
        Ok(())
    }

    pub async fn run_cdc_test(&self, start_millis: u64, parse_millis: u64) -> anyhow::Result<()> {
        self.src_to_dst_runner.execute_prepare_sqls().await?;
        self.expect_to_dst_runner.execute_prepare_sqls().await?;

        // start cdc task
        let task = self
            .src_to_dst_runner
            .spawn_cdc_task(start_millis, parse_millis)
            .await?;

        // load dml sqls
        let (src_insert_sqls, src_update_sqls, src_delete_sqls) =
            RdbTestRunner::split_dml_sqls(&self.src_to_dst_runner.base.src_test_sqls);

        let (expect_insert_sqls, expect_update_sqls, expect_delete_sqls) =
            RdbTestRunner::split_dml_sqls(&self.expect_to_dst_runner.base.src_test_sqls);

        let (expect_db_tbs, dst_db_tbs) = self.expect_to_dst_runner.get_compare_db_tbs()?;

        // insert src data
        if !src_insert_sqls.is_empty() {
            self.src_to_dst_runner
                .execute_src_sqls(&src_insert_sqls)
                .await?;
            TimeUtil::sleep_millis(parse_millis).await;

            self.expect_to_dst_runner
                .execute_src_sqls(&expect_insert_sqls)
                .await?;

            self.expect_to_dst_runner
                .compare_data_for_tbs(&expect_db_tbs, &dst_db_tbs)
                .await?;
        }

        // update src data
        if !src_update_sqls.is_empty() {
            self.src_to_dst_runner
                .execute_src_sqls(&src_update_sqls)
                .await?;
            TimeUtil::sleep_millis(parse_millis).await;

            self.expect_to_dst_runner
                .execute_src_sqls(&expect_update_sqls)
                .await?;

            self.expect_to_dst_runner
                .compare_data_for_tbs(&expect_db_tbs, &dst_db_tbs)
                .await?;
        }

        // delete src data
        if !src_delete_sqls.is_empty() {
            self.src_to_dst_runner
                .execute_src_sqls(&src_delete_sqls)
                .await?;
            TimeUtil::sleep_millis(parse_millis).await;

            self.expect_to_dst_runner
                .execute_src_sqls(&expect_delete_sqls)
                .await?;

            self.expect_to_dst_runner
                .compare_data_for_tbs(&expect_db_tbs, &dst_db_tbs)
                .await?;
        }

        self.src_to_dst_runner.base.wait_task_finish(&task).await
    }
}
