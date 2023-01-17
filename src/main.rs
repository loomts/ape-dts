use async_std::path::PathBuf;
use config::{
    mysql_to_rdb_cdc_config::MysqlToRdbCdcConfig,
    rdb_to_rdb_snapshot_config::RdbToRdbSnapshotConfig,
};
use error::Error;
use futures::executor::block_on;
use std::{env, fs::File, io::Read};
use task::{
    mysql_cdc_task::MysqlCdcTask, mysql_snapshot_task::MysqlSnapshotTask, task_type::TaskType,
};

mod config;
mod error;
mod ext;
mod extractor;
mod meta;
mod sinker;
mod task;

fn main() {
    let args: Vec<String> = env::args().collect();
    println!("{:?}", args);
    let config_name = &args[1];
    let task_type = &args[2];

    let _ = block_on(start_task(config_name, task_type));
}

async fn start_task(config_name: &str, task_type_str: &str) -> Result<(), Error> {
    let config_str = read_config(config_name).await.unwrap();
    let task_type = TaskType::from_name(task_type_str);

    match task_type {
        TaskType::MysqlToMysqlCdc => {
            let config = MysqlToRdbCdcConfig::from_str(&config_str).unwrap();
            MysqlCdcTask { config }.start().await.unwrap();
        }

        TaskType::MysqlToMysqlSnapshot => {
            let config = RdbToRdbSnapshotConfig::from_str(&config_str).unwrap();
            MysqlSnapshotTask { config }.start().await.unwrap();
        }

        _ => {}
    }

    Ok(())
}

async fn read_config(config_name: &str) -> Result<String, Error> {
    let mut config_file = File::open(PathBuf::from(config_name))?;
    let mut config_str = String::new();
    config_file.read_to_string(&mut config_str)?;
    Ok(config_str)
}
