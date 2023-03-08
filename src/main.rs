use crate::task::task_runner::TaskRunner;
use std::env;

mod config;
mod error;
mod extractor;
mod meta;
mod metric;
mod monitor;
mod sinker;
mod sqlx_ext;
mod task;
mod test;
mod traits;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        panic!("no task_config provided in args");
    }

    let task_config = args[1].clone();
    let runner = TaskRunner::new(task_config).await;
    runner.start_task(true).await.unwrap()
}
