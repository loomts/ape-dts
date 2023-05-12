use std::env;

use task_runner::TaskRunner;

mod extractor_util;
mod pipeline_util;
mod sinker_util;
mod task_runner;
mod task_util;
mod tests;

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
