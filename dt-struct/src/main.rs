use dt_common::config::config_loader::ConfigLoader;
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
    let (extractor_config, sinker_config, _, filter_config, router_config) =
        ConfigLoader::load(&task_config).unwrap();

    let builder = StructBuilder {
        extractor_config: extractor_config,
        sinker_config: sinker_config,
        filter_config: filter_config,
        router_config: router_config,
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        // .worker_threads(1) // used when new_multi_thread
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async { builder.build_job().await });
    println!("finished");
}
