use std::collections::{HashMap, HashSet};

use super::{
    mongo_test_runner::MongoTestRunner, rdb_check_test_runner::RdbCheckTestRunner,
    rdb_precheck_test_runner::RdbPrecheckTestRunner, rdb_struct_test_runner::RdbStructTestRunner,
    rdb_test_runner::RdbTestRunner,
};

pub struct TestBase {}

#[allow(dead_code)]
impl TestBase {
    pub async fn run_snapshot_test(test_dir: &str) {
        let runner = RdbTestRunner::new(test_dir).await.unwrap();
        runner.run_snapshot_test().await.unwrap();
    }

    pub async fn run_cdc_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let runner = RdbTestRunner::new(test_dir).await.unwrap();
        runner
            .run_cdc_test(start_millis, parse_millis)
            .await
            .unwrap();
        // runner.run_cdc_test_with_different_configs(start_millis, parse_millis))
        //     .unwrap();
    }

    pub async fn run_check_test(test_dir: &str) {
        let runner = RdbCheckTestRunner::new(test_dir).await.unwrap();
        runner.run_check_test().await.unwrap();
    }

    pub async fn run_review_test(test_dir: &str) {
        let runner = RdbCheckTestRunner::new(test_dir).await.unwrap();
        runner.run_review_test().await.unwrap();
    }

    pub async fn run_revise_test(test_dir: &str) {
        let runner = RdbCheckTestRunner::new(test_dir).await.unwrap();
        runner.run_revise_test().await.unwrap();
    }

    pub async fn run_mongo_snapshot_test(test_dir: &str) {
        let runner = MongoTestRunner::new(test_dir).await.unwrap();
        runner.run_snapshot_test().await.unwrap();
    }

    pub async fn run_mongo_cdc_test(test_dir: &str, start_millis: u64, parse_millis: u64) {
        let runner = MongoTestRunner::new(test_dir).await.unwrap();
        runner
            .run_cdc_test(start_millis, parse_millis)
            .await
            .unwrap();
    }

    pub async fn run_mysql_struct_test(test_dir: &str) {
        let mut runner = RdbStructTestRunner::new(test_dir).await.unwrap();
        runner.run_mysql_struct_test().await.unwrap();
    }

    pub async fn run_pg_struct_test(test_dir: &str) {
        let mut runner = RdbStructTestRunner::new(test_dir).await.unwrap();
        runner.run_pg_struct_test().await.unwrap();
    }

    pub async fn run_precheck_test(
        test_dir: &str,
        ignore_check_items: &HashSet<String>,
        src_expected_results: &HashMap<String, bool>,
        dst_expected_results: &HashMap<String, bool>,
    ) {
        let runner = RdbPrecheckTestRunner::new(test_dir).await.unwrap();
        runner.base.execute_test_ddl_sqls().await.unwrap();
        let results = runner.run_check().await;

        runner
            .validate(
                &results,
                ignore_check_items,
                &src_expected_results,
                &dst_expected_results,
            )
            .await;
    }
}
