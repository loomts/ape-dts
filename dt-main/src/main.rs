use std::env;

use dt_precheck::{config::task_config::PrecheckTaskConfig, do_precheck};
use dt_task::task_runner::TaskRunner;

#[tokio::main]
async fn main() {
    // let args: Vec<String> = env::args().collect();
    // if args.len() < 2 {
    //     panic!("no task_config provided in args");
    // }

    // let task_config = args[1].clone();
    let task_config = String::from(
        "/Users/caiqinyu/Desktop/Project/rust/ape-dts/dt-common/src/test/config/cdc_config.ini",
    );

    if PrecheckTaskConfig::new(&task_config).is_ok() {
        do_precheck(&task_config).await;
    } else {
        let runner = TaskRunner::new(task_config).await;
        runner.start_task(true).await.unwrap()
    }
}
