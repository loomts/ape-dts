use config::{
    mysql_to_rdb_cdc_config::MysqlToRdbCdcConfig,
    rdb_to_rdb_snapshot_config::RdbToRdbSnapshotConfig,
};
use dotenv::dotenv;
use error::Error;
use futures::executor::block_on;
use log::info;
use std::path::PathBuf;
use std::{fs::File, io::Read};
use task::{
    mysql_cdc_task::MysqlCdcTask, mysql_snapshot_task::MysqlSnapshotTask, task_type::TaskType,
};

use crate::config::env_var::EnvVar;

mod config;
mod error;
mod ext;
mod extractor;
mod meta;
mod sinker;
mod task;
mod test;

fn main() {
    dotenv().ok();
    let env_var = EnvVar::new().unwrap();

    log4rs::init_file(env_var.log4rs_file.clone(), Default::default()).unwrap();
    info!(
        "start task, config: {}, type: {}",
        env_var.task_config, env_var.task_type
    );

    let _ = block_on(start_task(&env_var));
}

async fn start_task(env_var: &EnvVar) -> Result<(), Error> {
    let mut config_str = String::new();
    File::open(PathBuf::from(env_var.task_config.clone()))?.read_to_string(&mut config_str)?;

    match TaskType::from_name(&env_var.task_type) {
        TaskType::MysqlToMysqlCdc => {
            let config = MysqlToRdbCdcConfig::from_str(&config_str).unwrap();
            MysqlCdcTask {
                config,
                env_var: env_var.clone(),
            }
            .start()
            .await
            .unwrap();
        }

        TaskType::MysqlToMysqlSnapshot => {
            let config = RdbToRdbSnapshotConfig::from_str(&config_str).unwrap();
            MysqlSnapshotTask {
                config,
                env_var: env_var.clone(),
            }
            .start()
            .await
            .unwrap();
        }

        _ => {}
    }

    Ok(())
}
