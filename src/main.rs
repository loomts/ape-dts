use crate::task::task_runner::TaskRunner;
use dotenv::dotenv;
use std::env;

mod config;
mod error;
mod extractor;
mod meta;
mod sinker;
mod task;
mod test;
mod traits;

const TASK_CONFIG: &str = "TASK_CONFIG";

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let task_config = if args.len() > 1 {
        args[1].clone()
    } else {
        dotenv().ok();
        env::var(TASK_CONFIG).unwrap()
    };

    TaskRunner::start_task(&task_config).await.unwrap()
}
