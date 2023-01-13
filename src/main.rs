use config::{rdb_to_rdb_config::RdbToRdbConfig, task_type::TaskType};
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
    // let config_file = "src/test/snapshot_test_config.yaml";
    let config_file = "src/test/cdc_test_config.yaml";
    let _ = block_on(start_task(config_file));
}

async fn start_task(config_file: &str) {
    let config_file = env::current_dir().unwrap().join(config_file);
    let config = RdbToRdbConfig::from_file(config_file.to_str().unwrap()).unwrap();
    match config.task_type {
        TaskType::MysqlToMysqlCdc => {
            MysqlCdcTask { config }.start().await.unwrap();
        }

        TaskType::MysqlToMysqlSnapshot => {
            MysqlSnapshotTask { config }.start().await.unwrap();
        }
    }
}
