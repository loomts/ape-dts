use std::{collections::HashSet, fs::File};

use dt_common::error::Error;

use crate::tests::test_config_util::TestConfigUtil;

use super::{base_test_runner::BaseTestRunner, rdb_test_runner::RdbTestRunner};

pub struct RdbCheckTestRunner {
    base: RdbTestRunner,
    dst_check_log_dir: String,
    expect_check_log_dir: String,
}

impl RdbCheckTestRunner {
    pub async fn new(relative_test_dir: &str) -> Result<Self, Error> {
        let base = RdbTestRunner::new(relative_test_dir).await.unwrap();
        let updated_config_fields = &base.base.updated_config_fields;

        let expect_check_log_dir = format!("{}/expect_check_log", &base.base.test_dir);

        let dst_check_log_dir = updated_config_fields
            .get(TestConfigUtil::SINKER_CHECK_LOG_DIR)
            .unwrap()
            .clone();

        Ok(Self {
            base,
            dst_check_log_dir: dst_check_log_dir.to_string(),
            expect_check_log_dir,
        })
    }

    pub async fn run_check_test(&self) -> Result<(), Error> {
        // clear existed check logs
        self.clear_check_log();

        // prepare src and dst tables
        self.base.execute_test_ddl_sqls().await?;
        self.base.execute_test_dml_sqls().await?;

        // start task
        self.base.base.start_task().await?;

        // check result
        let (expect_miss_logs, expect_diff_logs) = Self::load_check_log(&self.expect_check_log_dir);
        let (actual_miss_logs, actual_diff_logs) = Self::load_check_log(&self.dst_check_log_dir);

        assert_eq!(expect_diff_logs.len(), actual_diff_logs.len());
        assert_eq!(expect_miss_logs.len(), actual_miss_logs.len());
        for log in expect_diff_logs {
            assert!(actual_diff_logs.contains(&log))
        }
        for log in expect_miss_logs {
            assert!(actual_miss_logs.contains(&log))
        }

        Ok(())
    }

    pub async fn run_revise_test(&self) -> Result<(), Error> {
        self.clear_check_log();
        self.base.run_snapshot_test().await
    }

    pub async fn run_review_test(&self) -> Result<(), Error> {
        self.clear_check_log();
        self.run_check_test().await
    }

    fn load_check_log(log_dir: &str) -> (HashSet<String>, HashSet<String>) {
        let miss_log_file = format!("{}/miss.log", log_dir);
        let diff_log_file = format!("{}/diff.log", log_dir);
        let mut miss_logs = HashSet::new();
        let mut diff_logs = HashSet::new();

        for log in BaseTestRunner::load_file(&miss_log_file) {
            miss_logs.insert(log);
        }
        for log in BaseTestRunner::load_file(&diff_log_file) {
            diff_logs.insert(log);
        }
        (miss_logs, diff_logs)
    }

    fn clear_check_log(&self) {
        let miss_log_file = format!("{}/miss.log", self.dst_check_log_dir);
        let diff_log_file = format!("{}/diff.log", self.dst_check_log_dir);
        if BaseTestRunner::check_file_exists(&miss_log_file) {
            File::create(&miss_log_file).unwrap().set_len(0).unwrap();
        }
        if BaseTestRunner::check_file_exists(&diff_log_file) {
            File::create(&diff_log_file).unwrap().set_len(0).unwrap();
        }
    }
}
