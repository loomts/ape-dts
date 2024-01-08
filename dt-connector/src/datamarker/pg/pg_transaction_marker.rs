use dt_common::{
    datamarker::transaction_control::{TopologyInfo, TransactionWorker},
    error::Error,
};
use dt_meta::{dt_data::DtData, row_data::RowData};

use crate::datamarker::{rdb_basic::RdbBasicTransactionMarker, traits::DataMarkerFilter};

pub struct PgTransactionMarker {
    pub rdb_basic_marker: RdbBasicTransactionMarker,
}

impl PgTransactionMarker {
    pub fn new(transaction_worker: TransactionWorker, current_topology: TopologyInfo) -> Self {
        PgTransactionMarker {
            rdb_basic_marker: RdbBasicTransactionMarker::new(transaction_worker, current_topology),
        }
    }
}

impl DataMarkerFilter for PgTransactionMarker {
    fn filter_dtdata(&mut self, data: &DtData) -> Result<bool, Error> {
        self.rdb_basic_marker.filter_dtdata(data)
    }

    fn filter_rowdata(&mut self, row_data: &RowData) -> Result<bool, Error> {
        self.rdb_basic_marker.filter_rowdata(row_data)
    }
}
