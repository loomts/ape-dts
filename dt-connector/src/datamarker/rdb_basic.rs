use std::collections::HashMap;

use dt_common::{
    datamarker::transaction_control::{TopologyInfo, TransactionWorker},
    error::Error,
    log_info,
};
use dt_meta::{dt_data::DtData, row_data::RowData};

pub struct RdbBasicTransactionMarker {
    pub transaction_worker: TransactionWorker,
    pub current_topology: TopologyInfo,
    pub do_transaction_filter: bool,

    pub cache: HashMap<(String, String), bool>,
}

impl RdbBasicTransactionMarker {
    pub fn new(transaction_worker: TransactionWorker, current_topology: TopologyInfo) -> Self {
        RdbBasicTransactionMarker {
            transaction_worker,
            current_topology,
            do_transaction_filter: false,
            cache: HashMap::new(),
        }
    }
}

impl RdbBasicTransactionMarker {
    pub fn filter_dtdata(&mut self, data: &DtData) -> Result<bool, Error> {
        match data {
            DtData::Dml { row_data } => return self.filter_rowdata(row_data),

            DtData::Commit { .. } => {
                let old_filter_flag = self.do_transaction_filter;
                self.do_transaction_filter = false;
                return Ok(old_filter_flag);
            }
            _ => (),
        }

        Ok(self.do_transaction_filter)
    }

    pub fn filter_rowdata(&mut self, row_data: &RowData) -> Result<bool, Error> {
        if self.do_transaction_filter {
            return Ok(true);
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

                    return Ok(true);
                }
            }
            Err(e) => return Err(e),
        }

        Ok(self.do_transaction_filter)
    }
}
