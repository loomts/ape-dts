use std::collections::HashMap;

use super::{col_value::ColValue, row_type::RowType};

pub struct RowData {
    pub db: String,
    pub tb: String,
    pub row_type: RowType,
    pub before: Option<HashMap<String, ColValue>>,
    pub after: Option<HashMap<String, ColValue>>,
}
