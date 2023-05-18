use std::collections::{HashMap, HashSet};

use dt_common::{config::task_config::TaskConfig, error::Error};

use dt_precheck::{
    config::task_config::PrecheckTaskConfig, connector::checker_connector::CheckerConnector,
    meta::check_result::CheckResult,
};

use super::rdb_test_runner::RdbTestRunner;

pub struct RdbPrecheckTestRunner {
    pub base: RdbTestRunner,
    checker_connector: CheckerConnector,
}

impl RdbPrecheckTestRunner {
    pub async fn new(relative_test_dir: &str) -> Result<Self, Error> {
        let base = RdbTestRunner::new(relative_test_dir).await.unwrap();

        let task_config = TaskConfig::new(&base.base.task_config_file);
        let precheck_config = PrecheckTaskConfig::new(&base.base.task_config_file).unwrap();
        let checker_connector =
            CheckerConnector::build(precheck_config.precheck.clone(), task_config.clone());

        Ok(Self {
            base,
            checker_connector,
        })
    }

    pub async fn run_check(&self) -> Vec<Result<CheckResult, dt_precheck::error::Error>> {
        self.checker_connector.check().await.unwrap()
    }

    pub async fn validate(
        &self,
        results: &Vec<Result<CheckResult, dt_precheck::error::Error>>,
        ignore_check_items: &HashSet<String>,
        src_expected_results: &HashMap<String, bool>,
        dst_expected_results: &HashMap<String, bool>,
    ) {
        let compare = |result: &CheckResult, expected_results: &HashMap<String, bool>| {
            if let Some(expected) = expected_results.get(&result.check_type_name) {
                assert_eq!(&result.is_validate, expected);
            } else {
                // by default, is_validate == true
                assert!(&result.is_validate);
            }
        };

        for i in results.iter() {
            let result = i.as_ref().unwrap();

            if ignore_check_items.contains(&result.check_type_name) {
                continue;
            }

            println!(
                "comparing precheck result, item: {}, is_source: {}",
                result.check_type_name, result.is_source
            );

            if result.is_source {
                compare(result, src_expected_results);
            } else {
                compare(result, dst_expected_results);
            }
        }
    }
}
