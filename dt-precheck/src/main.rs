use std::env;

use dt_common::config::task_config::TaskConfig;

use crate::{
    config::task_config::PrecheckTaskConfig, connector::checker_connector::CheckerConnector,
};

mod checker;
mod config;
mod connector;
mod error;
mod meta;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        panic!("no task_config provided in args");
    }
    let config = args[1].clone();

    let task_config = TaskConfig::new(&config);
    let precheck_config = PrecheckTaskConfig::new(&config);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let checker_connector = CheckerConnector::build(precheck_config.precheck, task_config);
        let result = checker_connector.check().await;
        match result {
            Err(e) => {
                println!("precheck not passed.");
                panic!("precheck meet error: {}", e);
            }
            _ => {}
        }
    });
    println!("precheck passed.");
}
