use super::{ddl_data::DdlData, row_data::RowData};

#[derive(Debug, Clone)]
pub enum DtData {
    Ddl { ddl_data: DdlData },
    Dml { row_data: RowData },
    Commit { xid: String, position: String },
}

impl DtData {
    pub fn is_ddl(&self) -> bool {
        matches!(self, DtData::Ddl { .. })
    }
}
