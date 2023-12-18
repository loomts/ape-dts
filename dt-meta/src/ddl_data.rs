use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::struct_meta::statement::struct_statement::StructStatement;

use super::ddl_type::DdlType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DdlData {
    pub schema: String,
    pub tb: String,
    pub query: String,
    #[serde(skip)]
    pub statement: Option<StructStatement>,
    #[serde(skip)]
    pub ddl_type: DdlType,
}

impl DdlData {
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        json!(self).to_string()
    }
}
