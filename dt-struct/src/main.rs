use dt_common::config::task_config::TaskConfig;
use factory::database_worker_builder::StructBuilder;
use meta::common::database_model::StructModel;
use std::env;

mod extractor;
mod factory;
mod meta;
mod sinker;
mod traits;
mod utils;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        panic!("no task_config provided in args");
    }
    let task_config = args[1].clone();
    // let task_config = String::from(
    //     "/Users/caiqinyu/Desktop/Project/rust/ape-dts/dt-struct/src/test/task_config.ini",
    // );
    let task_config = TaskConfig::new(&task_config);

    let builder = StructBuilder {
        extractor_config: task_config.extractor,
        sinker_config: task_config.sinker,
        filter_config: task_config.filter,
        router_config: task_config.router,
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        // .worker_threads(1) // used when new_multi_thread
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async { builder.build_job().await });
    println!("finished");
}
