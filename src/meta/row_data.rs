use std::collections::HashMap;

use super::{col_value::ColValue, position_info::PositionInfo, row_type::RowType, tb_meta::TbMeta};

#[derive(Debug, Clone)]
pub struct RowData {
    pub db: String,
    pub tb: String,
    pub row_type: RowType,
    pub before: Option<HashMap<String, ColValue>>,
    pub after: Option<HashMap<String, ColValue>>,
    pub position_info: Option<PositionInfo>,
}

impl RowData {
    pub fn to_string(&self, tb_meta: &TbMeta) -> String {
        let mut result = Vec::new();
        result.push(format!(
            "db: {}, tb: {}, row_type: {}",
            self.db,
            self.tb,
            self.row_type.to_str()
        ));

        result.push(format!(
            "before: {}",
            Self::format_values(&self.before, tb_meta)
        ));

        result.push(format!(
            "after: {}",
            Self::format_values(&self.after, tb_meta)
        ));

        result.join("\n")
    }

    fn format_values(values: &Option<HashMap<String, ColValue>>, tb_meta: &TbMeta) -> String {
        if values.is_none() {
            return "none".to_string();
        }

        let mut result = Vec::new();
        let unwrap_values = values.as_ref().unwrap();
        for col in &tb_meta.cols {
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
