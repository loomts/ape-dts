use std::collections::HashMap;

use super::{col_value::ColValue, row_type::RowType};

#[derive(Debug, Clone)]
pub struct RowData {
    pub db: String,
    pub tb: String,
    pub row_type: RowType,
    pub before: Option<HashMap<String, ColValue>>,
    pub after: Option<HashMap<String, ColValue>>,
    pub current_position: String,
    // for mysql, this is the binlog position of the last commited transaction
    // for postgres, this is the sln of the last commited commited transaction
    pub checkpoint_position: String,
}

impl RowData {
    pub fn to_string(&self, cols: &Vec<String>) -> String {
        let mut result = Vec::new();
        result.push(format!(
            "db: {}, tb: {}, row_type: {}",
            self.db,
            self.tb,
            self.row_type.to_str()
        ));
        result.push(format!(
            "before: {}",
            Self::format_values(&self.before, cols)
        ));
        result.push(format!("after: {}", Self::format_values(&self.after, cols)));
        result.join("\n")
    }

    fn format_values(values: &Option<HashMap<String, ColValue>>, cols: &Vec<String>) -> String {
        if values.is_none() {
            return "none".to_string();
        }

        let mut result = Vec::new();
        let unwrap_values = values.as_ref().unwrap();
        for col in cols.iter() {
            if !unwrap_values.contains_key(col) {
                result.push(format!("{}: none", col));
                continue;
            }

            result.push(format!(
                "{}: {}",
                col,
                unwrap_values.get(col).unwrap().to_string()
            ));
        }
        return result.join(" , ");
    }
}
