use tokio::runtime::Runtime;

use super::test_runners::{
    mongo_test_runner::MongoTestRunner, rdb_check_test_runner::RdbCheckTestRunner,
    rdb_test_runner::RdbTestRunner,
};

pub struct TestBase {}

#[allow(dead_code)]
impl TestBase {
    pub fn run_snapshot_test(test_dir: &str) {
        let rt = Runtime::new().unwrap();
        let runner = rt.block_on(RdbTestRunner::new(test_dir)).unwrap();
        rt.block_on(runner.run_snapshot_test()).unwrap();
    }

    pub fn run_cdc_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let rt = Runtime::new().unwrap();
        let runner = rt.block_on(RdbTestRunner::new(test_dir)).unwrap();

        rt.block_on(runner.run_cdc_test(start_millis, parse_millis))
            .unwrap();
        // rt.block_on(runner.run_cdc_test_with_different_configs(start_millis, parse_millis))
        //     .unwrap();
    }

    pub fn run_check_test(test_dir: &str) {
        let rt = Runtime::new().unwrap();
        let runner = rt.block_on(RdbCheckTestRunner::new(test_dir)).unwrap();
        rt.block_on(runner.run_check_test()).unwrap();
    }

    pub fn run_review_test(test_dir: &str) {
        let rt = Runtime::new().unwrap();
        let runner = rt.block_on(RdbCheckTestRunner::new(test_dir)).unwrap();
        rt.block_on(runner.run_review_test()).unwrap();
    }

    pub fn run_revise_test(test_dir: &str) {
        let rt = Runtime::new().unwrap();
        let runner = rt.block_on(RdbCheckTestRunner::new(test_dir)).unwrap();
        rt.block_on(runner.run_revise_test()).unwrap();
    }

    pub fn run_mongo_snapshot_test(test_dir: &str) {
        let rt = Runtime::new().unwrap();
        let runner = rt.block_on(MongoTestRunner::new(test_dir)).unwrap();
        rt.block_on(runner.run_snapshot_test()).unwrap();
    }

    pub fn run_mongo_cdc_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let rt = Runtime::new().unwrap();
        let runner = rt.block_on(MongoTestRunner::new(test_dir)).unwrap();
        rt.block_on(runner.run_cdc_test(start_millis, parse_millis))
            .unwrap();
    }
}
