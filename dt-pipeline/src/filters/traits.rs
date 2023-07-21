use dt_common::error::Error;
use dt_meta::{dt_data::DtData, row_data::RowData};

pub trait TransactionFilter {
    fn filter_dmls(
        &mut self,
        datas: Vec<DtData>,
    ) -> Result<(Vec<RowData>, Option<String>, Option<String>), Error>;
}
