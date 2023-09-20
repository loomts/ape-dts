use std::collections::HashMap;

use dt_common::{
    config::extractor_config::ExtractorConfig,
    error::Error,
    utils::transaction_circle_control::{TopologyInfo, TransactionWorker},
};

use crate::drainers::{mysql_transaction_filter::MysqlTransactionFilter, traits::DataDrainer};

pub struct DrainerUtil {}

impl DrainerUtil {
    pub fn create_transaction_filter_drainer(
        extractor: &ExtractorConfig,
        transaction_worker: TransactionWorker,
        current_topology: TopologyInfo,
    ) -> Result<Box<dyn DataDrainer + Send>, Error> {
        match extractor {
            ExtractorConfig::MysqlCdc { .. } => Ok(Box::new(MysqlTransactionFilter {
                transaction_worker,
                current_topology,
                do_transaction_filter: false,
                cache: HashMap::new(),
            })),
            _ => Err(Error::ConfigError(String::from(
                "extractor type not support transaction filter yet.",
            ))),
        }
    }
}
