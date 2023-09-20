use dt_common::{
    config::{extractor_config::ExtractorConfig, pipeline_config::PipelineConfig},
    error::Error,
    log_error,
    utils::transaction_circle_control::TransactionWorker,
};
use dt_pipeline::{
    drainers::{basic::BasicDataDrainer, traits::DataDrainer},
    utils::drainer_util::DrainerUtil,
};

pub struct PipelineUtil {}

impl PipelineUtil {
    pub fn build_drainer(
        pipeline_config: PipelineConfig,
        extractor_config: ExtractorConfig,
    ) -> Result<Box<dyn DataDrainer + Send>, Error> {
        let drainer: Box<dyn DataDrainer + Send> = match pipeline_config.get_pipeline_type() {
            dt_common::config::config_enums::PipelineType::Transaction => {
                let worker = TransactionWorker::from(&pipeline_config);
                if !worker.is_validate() {
                    log_error!("transaction config is invalid when gernate TransactionWorker.");
                    return Err(Error::ConfigError(String::from(
                        "transaction config is invalid",
                    )));
                }
                let result = worker.pick_infos(&worker.transaction_db, &worker.transaction_table);
                let topology = result.unwrap().unwrap();
                if topology.is_empty() {
                    log_error!("transaction config is invalid, topology info is empty.");
                    return Err(Error::ConfigError(String::from(
                        "transaction config is invalid, topology info is empty",
                    )));
                }

                DrainerUtil::create_transaction_filter_drainer(&extractor_config, worker, topology)?
            }
            _ => Box::new(BasicDataDrainer {}),
        };

        Ok(drainer)
    }
}
