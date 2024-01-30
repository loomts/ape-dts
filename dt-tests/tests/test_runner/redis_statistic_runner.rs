use std::{collections::HashSet, fs::File};

use dt_common::{config::sinker_config::SinkerConfig, error::Error};

use super::base_test_runner::BaseTestRunner;

pub struct RedisStatisticTestRunner {
    pub base: BaseTestRunner,
}

impl RedisStatisticTestRunner {
    pub async fn new(relative_test_dir: &str) -> Result<Self, Error> {
        let base = BaseTestRunner::new(relative_test_dir).await.unwrap();
        Ok(Self { base })
    }

    pub async fn run_statistic_test(&mut self) -> Result<(), Error> {
        let dst_statistic_file = match self.base.get_config().sinker {
            SinkerConfig::RedisStatistic {
                statistic_log_dir, ..
            } => format!("{}/statistic.log", statistic_log_dir),
            _ => String::new(),
        };
        Self::clear_statistic_log(&dst_statistic_file);

        let expect_statistic_file =
            format!("{}/expect_statistic_log/statistic.log", self.base.test_dir);

        // start task
        self.base.start_task().await?;

        let mut expect_logs = HashSet::new();
        let mut dst_logs = HashSet::new();
        for log in BaseTestRunner::load_file(&expect_statistic_file) {
            expect_logs.insert(log);
        }
        for log in BaseTestRunner::load_file(&dst_statistic_file) {
            dst_logs.insert(log);
        }

        assert_eq!(dst_logs, expect_logs);
        Ok(())
    }

    pub fn clear_statistic_log(dst_statistic_file: &str) {
        if BaseTestRunner::check_path_exists(&dst_statistic_file) {
            File::create(&dst_statistic_file)
                .unwrap()
                .set_len(0)
                .unwrap();
        }
    }
}
