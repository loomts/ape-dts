use std::fs::File;

use dt_common::{error::Error, utils::time_util::TimeUtil};

use crate::test_config_util::TestConfigUtil;

use super::{base_test_runner::BaseTestRunner, rdb_test_runner::RdbTestRunner};

/// This is used for test cases: rdb(src) -> sql log.
/// We need one task runner to generate sql log
///     rdb(src) -> sql log
/// And we need another dummy task runner to compare rdb(src) and rdb(dst)
///     rdb(src) -> rdb(dst)
pub struct RdbSqlTestRunner {
    src_to_sql_runner: RdbTestRunner,
    src_to_dst_runner: RdbTestRunner,
    reverse: bool,
}

#[allow(dead_code)]
impl RdbSqlTestRunner {
    pub async fn new(relative_test_dir: &str, reverse: bool) -> Result<Self, Error> {
        let src_to_sql_runner =
            RdbTestRunner::new_default(&format!("{}/src_to_sql", relative_test_dir)).await?;
        let src_to_dst_runner =
            RdbTestRunner::new(&format!("{}/src_to_dst", relative_test_dir), false).await?;
        Ok(Self {
            src_to_sql_runner,
            src_to_dst_runner,
            reverse,
        })
    }

    pub async fn close(&self) -> Result<(), Error> {
        self.src_to_sql_runner.close().await?;
        self.src_to_dst_runner.close().await
    }

    pub async fn run_cdc_to_sql_test(
        &self,
        start_millis: u64,
        parse_millis: u64,
    ) -> Result<(), Error> {
        if self.reverse {
            self.update_reverse_config();
        }

        let mut gernated_sqls = self.generate_sqls(start_millis, parse_millis).await?;
        if self.reverse {
            gernated_sqls.reverse();
        }

        // 1, clear src and dst
        // 2, execute src_to_sql/src_test_sqls in src
        // 3, execute gernerated_sqls in dst
        // 4, compare src and dst, make sure src_test_sqls and gernerated_sqls generate same data
        let (src_db_tbs, dst_db_tbs) = self.src_to_dst_runner.get_compare_db_tbs().await?;
        let (src_insert_sqls, src_update_sqls, src_delete_sqls) =
            RdbTestRunner::split_dml_sqls(&self.src_to_sql_runner.base.src_test_sqls);
        let (dst_insert_sqls, dst_update_sqls, dst_delete_sqls) =
            RdbTestRunner::split_dml_sqls(&gernated_sqls);

        if !self.reverse {
            self.src_to_dst_runner.execute_prepare_sqls().await?;
            // insert
            self.src_to_dst_runner
                .execute_src_sqls(&src_insert_sqls)
                .await?;
            self.src_to_dst_runner
                .execute_dst_sqls(&dst_insert_sqls)
                .await?;
            self.src_to_dst_runner
                .compare_data_for_tbs(&src_db_tbs, &dst_db_tbs)
                .await?;

            // update
            self.src_to_dst_runner
                .execute_src_sqls(&src_update_sqls)
                .await?;
            self.src_to_dst_runner
                .execute_dst_sqls(&dst_update_sqls)
                .await?;
            self.src_to_dst_runner
                .compare_data_for_tbs(&src_db_tbs, &dst_db_tbs)
                .await?;

            // delete
            self.src_to_dst_runner
                .execute_src_sqls(&src_delete_sqls)
                .await?;
            self.src_to_dst_runner
                .execute_dst_sqls(&dst_delete_sqls)
                .await?;
            self.src_to_dst_runner
                .compare_data_for_tbs(&src_db_tbs, &dst_db_tbs)
                .await?;
        } else {
            self.src_to_dst_runner.execute_prepare_sqls().await?;
            // src: insert
            self.src_to_dst_runner
                .execute_src_sqls(&src_insert_sqls)
                .await?;
            // dst: insert + update
            self.src_to_dst_runner
                .execute_dst_sqls(&dst_insert_sqls)
                .await?;
            self.src_to_dst_runner
                .execute_dst_sqls(&dst_update_sqls)
                .await?;

            self.src_to_dst_runner
                .compare_data_for_tbs(&src_db_tbs, &dst_db_tbs)
                .await?;

            // src: update + delete
            self.src_to_dst_runner
                .execute_src_sqls(&src_update_sqls)
                .await?;
            self.src_to_dst_runner
                .execute_src_sqls(&src_delete_sqls)
                .await?;
            // dst: delete
            self.src_to_dst_runner
                .execute_dst_sqls(&dst_delete_sqls)
                .await?;

            self.src_to_dst_runner
                .compare_data_for_tbs(&src_db_tbs, &dst_db_tbs)
                .await?;

            // src: insert + update
            self.src_to_dst_runner
                .execute_src_sqls(&src_insert_sqls)
                .await?;
            self.src_to_dst_runner
                .execute_src_sqls(&src_update_sqls)
                .await?;
            // dst: insert
            self.src_to_dst_runner
                .execute_dst_sqls(&dst_insert_sqls)
                .await?;

            self.src_to_dst_runner
                .compare_data_for_tbs(&src_db_tbs, &dst_db_tbs)
                .await?;
        }

        Ok(())
    }

    async fn generate_sqls(
        &self,
        start_millis: u64,
        parse_millis: u64,
    ) -> Result<Vec<String>, Error> {
        // clear sql.log if exists
        let log_file = format!(
            "{}/sql.log",
            self.src_to_sql_runner.base.get_config().runtime.log_dir
        );
        if BaseTestRunner::check_path_exists(&log_file) {
            File::create(&log_file).unwrap().set_len(0).unwrap();
        }

        // start task to generate sql file
        self.src_to_sql_runner.execute_prepare_sqls().await?;

        let task = self.src_to_sql_runner.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;

        // execute sqls in src
        self.src_to_sql_runner
            .execute_src_sqls(&self.src_to_sql_runner.base.src_test_sqls)
            .await?;
        TimeUtil::sleep_millis(parse_millis).await;

        // stop generating sql log
        self.src_to_sql_runner.base.wait_task_finish(&task).await?;

        let gernated_sqls = BaseTestRunner::load_file(&log_file);
        assert!(!gernated_sqls.is_empty());
        Ok(gernated_sqls)
    }

    fn update_reverse_config(&self) {
        let update_configs = vec![(
            "sinker".to_string(),
            "reverse".to_string(),
            "true".to_string(),
        )];

        TestConfigUtil::update_task_config(
            &self.src_to_sql_runner.base.task_config_file,
            &self.src_to_sql_runner.base.task_config_file,
            &update_configs,
        );
    }
}
