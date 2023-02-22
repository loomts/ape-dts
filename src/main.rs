use config::{
    mysql_to_rdb_cdc_config::MysqlToRdbCdcConfig,
    rdb_to_rdb_snapshot_config::RdbToRdbSnapshotConfig,
};
use dotenv::dotenv;
use error::Error;
use futures::executor::block_on;
use log4rs::config::RawConfig;
use std::env;
use std::path::PathBuf;
use std::{fs::File, io::Read};
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
mod test;

const LOG_LEVEL_PLACEHODLER: &str = "LOG_LEVEL_PLACEHODLER";
const LOG_DIR_PLACEHODLER: &str = "LOG_DIR_PLACEHODLER";
const LOG4RS_YAML: &str = "log4rs.yaml";
const TASK_CONFIG: &str = "TASK_CONFIG";
const TASK_TYPE: &str = "TASK_TYPE";

fn main() {
    let args: Vec<String> = env::args().collect();
    let (task_config, task_type) = if args.len() > 2 {
        (args[1].clone(), args[2].clone())
    } else {
        dotenv().ok();
        (env::var(TASK_CONFIG).unwrap(), env::var(TASK_TYPE).unwrap())
    };

    let _ = block_on(start_task(&task_config, &task_type));
}

async fn start_task(task_config: &str, task_type: &str) -> Result<(), Error> {
    let mut config_str = String::new();
    File::open(PathBuf::from(task_config))?.read_to_string(&mut config_str)?;

    match TaskType::from_name(task_type) {
        TaskType::MysqlToMysqlCdc => {
            let config = MysqlToRdbCdcConfig::from_str(&config_str).unwrap();
            init_log4rs(&config.log_dir, &config.log_level)?;
            MysqlCdcTask { config }.start().await.unwrap();
        }

        TaskType::MysqlToMysqlSnapshot => {
            let config = RdbToRdbSnapshotConfig::from_str(&config_str).unwrap();
            init_log4rs(&config.log_dir, &config.log_level)?;
            MysqlSnapshotTask { config }.start().await.unwrap();
        }

        _ => {}
    }

    Ok(())
}

fn init_log4rs(log_dir: &str, log_level: &str) -> Result<(), Error> {
    let mut config_str = String::new();
    File::open(LOG4RS_YAML)?.read_to_string(&mut config_str)?;
    config_str = config_str
        .replace(LOG_DIR_PLACEHODLER, log_dir)
        .replace(LOG_LEVEL_PLACEHODLER, log_level);

    let config: RawConfig = serde_yaml::from_str(&config_str)?;
    log4rs::init_raw_config(config).unwrap();
    Ok(())
}
