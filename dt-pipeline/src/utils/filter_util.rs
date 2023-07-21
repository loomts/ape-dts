use std::collections::HashMap;

use dt_common::{
    config::extractor_config::ExtractorConfig, error::Error,
    utils::transaction_circle_control::TransactionWorker,
};

use crate::filters::{mysql_transaction_filter::MysqlTransactionFilter, traits::TransactionFilter};

pub struct FilterUtil {}

impl FilterUtil {
    pub fn create_transaction_filter(
        extractor: &ExtractorConfig,
        transaction_worker: TransactionWorker,
        current_topology_key: String,
    ) -> Result<Box<dyn TransactionFilter + Send>, Error> {
        match extractor {
            ExtractorConfig::MysqlCdc { .. } => Ok(Box::new(MysqlTransactionFilter {
                transaction_worker,
                current_topology_key,
                cache: HashMap::new(),
                do_transaction_filter: false,
            })),
            _ => Err(Error::ConfigError {
                error: String::from("extractor type not support transaction filter yet."),
            }),
        }
    }
}
