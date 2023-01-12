use config::rdb_to_rdb_config::RdbToRdbConfig;
use futures::executor::block_on;
use std::env;
use task::{mysql_cdc_task::MysqlCdcTask, mysql_snapshot_task::MysqlSnapshotTask};

mod config;
mod error;
mod ext;
mod extractor;
mod meta;
mod sinker;
mod task;

fn main() {
    let config_file = "src/test/snapshot_test_config.yaml";
    // let config_file = "src/test/cdc_test_config.yaml";

    let config_file = env::current_dir().unwrap().join(config_file);
    let config = RdbToRdbConfig::from_file(config_file.to_str().unwrap()).unwrap();

    // let task = MysqlCdcTask { config };
    let task = MysqlSnapshotTask { config };
    let _ = block_on(task.start());
}
