use tokio::runtime::Runtime;

use super::{test_config_util::TestConfigUtil, test_runner::TestRunner};

pub struct TestBase {}

#[allow(dead_code)]
impl TestBase {
    pub fn run_snapshot_test(test_dir: &str) {
        let rt = Runtime::new().unwrap();
        let runner = rt.block_on(TestRunner::new(test_dir)).unwrap();
        rt.block_on(runner.run_snapshot_test()).unwrap();
    }

    pub fn run_cdc_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let rt = Runtime::new().unwrap();
        let runner = rt.block_on(TestRunner::new(test_dir)).unwrap();
        let configs = TestConfigUtil::get_default_configs();
        rt.block_on(runner.run_cdc_test_with_different_configs(
            start_millis,
            parse_millis,
            &configs,
        ))
        .unwrap();
    }

    pub fn run_check_test(test_dir: &str) {
        let rt = Runtime::new().unwrap();
        let runner = rt.block_on(TestRunner::new(test_dir)).unwrap();
        rt.block_on(runner.run_check_test()).unwrap();
    }

    pub fn run_review_test(test_dir: &str) {
        let rt = Runtime::new().unwrap();
        let runner = rt.block_on(TestRunner::new(test_dir)).unwrap();
        rt.block_on(runner.run_review_test()).unwrap();
    }

    pub fn run_revise_test(test_dir: &str) {
        let rt = Runtime::new().unwrap();
        let runner = rt.block_on(TestRunner::new(test_dir)).unwrap();
        rt.block_on(runner.run_revise_test()).unwrap();
    }
}
