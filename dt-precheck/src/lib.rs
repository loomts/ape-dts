use dt_common::config::task_config::TaskConfig;

use crate::{
    config::task_config::PrecheckTaskConfig, connector::checker_connector::CheckerConnector,
};

pub mod checker;
pub mod config;
pub mod connector;
pub mod error;
pub mod fetcher;
pub mod meta;

pub async fn do_precheck(config: &str) {
    let task_config = TaskConfig::new(config);
    let precheck_config = PrecheckTaskConfig::new(config).unwrap();

    let checker_connector = CheckerConnector::build(precheck_config.precheck, task_config);
    let result = checker_connector.verify_check_result().await;
    if let Err(e) = result {
        println!("precheck not passed.");
        panic!("precheck meet error: {}", e);
    }

    println!("precheck passed.");
}
