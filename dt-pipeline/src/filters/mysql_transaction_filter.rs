use std::collections::HashMap;

use dt_common::{
    error::Error,
    log_info,
    utils::transaction_circle_control::{TopologyInfo, TransactionWorker},
};
use dt_meta::{dt_data::DtData, row_data::RowData};

use super::traits::TransactionFilter;

pub struct MysqlTransactionFilter {
    pub transaction_worker: TransactionWorker,
    pub current_topology: TopologyInfo,
    pub do_transaction_filter: bool,

    pub cache: HashMap<(String, String), bool>,
}

impl TransactionFilter for MysqlTransactionFilter {
    fn filter_dmls(
        &mut self,
        mut datas: Vec<DtData>,
    ) -> Result<(Vec<RowData>, Option<String>, Option<String>), Error> {
        let mut result_data = Vec::new();
        let mut last_received_position = Option::None;
        let mut last_commit_position = Option::None;

        for data in datas.drain(..) {
            match data {
                DtData::Dml { row_data } => {
                    last_received_position = Some(row_data.position.clone());

                    if self.do_transaction_filter {
                        continue;
                    }

                    match self.transaction_worker.is_filter(
                        &row_data.schema,
                        &row_data.tb,
                        self.current_topology.clone(),
                        &mut self.cache,
                    ) {
                        Ok((is_trans_event, is_filter, is_from_cache)) => {
                            if !is_trans_event {
                                self.do_transaction_filter = false;
                            } else {
                                self.do_transaction_filter = is_filter;

                                if is_filter && !is_from_cache {
                                    log_info!(
                                        "filted by transaction-table:[{},{}]",
                                        &row_data.schema,
                                        &row_data.tb
                                    );
                                }

                                continue;
                            }
                        }
                        Err(e) => return Err(e),
                    }

                    result_data.push(row_data)
                }

                DtData::Commit { position, .. } => {
                    last_commit_position = Some(position);
                    self.do_transaction_filter = false;
                }
                _ => (),
            }
        }

        Ok((result_data, last_received_position, last_commit_position))
    }
}
