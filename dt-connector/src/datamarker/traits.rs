use dt_common::error::Error;
use dt_meta::{dt_data::DtData, row_data::RowData};

pub trait DataMarkerFilter {
    fn filter_dtdata(&mut self, data: &DtData) -> Result<bool, Error>;

    fn filter_rowdata(&mut self, data: &RowData) -> Result<bool, Error>;
}
