use std::{collections::HashSet, fs::File};

use dt_common::error::Error;

use crate::test_config_util::TestConfigUtil;

use super::base_test_runner::BaseTestRunner;

pub struct CheckUtil {}

impl CheckUtil {
    pub fn validate_check_log(
        expect_check_log_dir: &str,
        dst_check_log_dir: &str,
    ) -> Result<(), Error> {
        // check result
        let (expect_miss_logs, expect_diff_logs) = Self::load_check_log(expect_check_log_dir);
        let (actual_miss_logs, actual_diff_logs) = Self::load_check_log(dst_check_log_dir);

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

    pub fn clear_check_log(dst_check_log_dir: &str) {
        let miss_log_file = format!("{}/miss.log", dst_check_log_dir);
        let diff_log_file = format!("{}/diff.log", dst_check_log_dir);
        if BaseTestRunner::check_file_exists(&miss_log_file) {
            File::create(&miss_log_file).unwrap().set_len(0).unwrap();
        }
        if BaseTestRunner::check_file_exists(&diff_log_file) {
            File::create(&diff_log_file).unwrap().set_len(0).unwrap();
        }
    }

    pub fn get_check_log_dir(base_test_runner: &BaseTestRunner) -> (String, String) {
        let expect_check_log_dir = format!("{}/expect_check_log", base_test_runner.test_dir);
        let dst_check_log_dir = base_test_runner
            .updated_config_fields
            .get(TestConfigUtil::SINKER_CHECK_LOG_DIR)
            .unwrap();
        (expect_check_log_dir, dst_check_log_dir.into())
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
}
