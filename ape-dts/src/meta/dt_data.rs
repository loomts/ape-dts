use super::row_data::RowData;

#[derive(Debug, Clone)]
pub enum DtData {
    Dml { row_data: RowData },
    Commit { xid: String, position: String },
}
