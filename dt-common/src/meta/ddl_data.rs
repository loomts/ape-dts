use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::meta::struct_meta::statement::struct_statement::StructStatement;

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

impl std::fmt::Display for DdlData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}
