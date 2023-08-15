use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{ddl_type::DdlType, struct_meta::database_model::StructModel};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DdlData {
    pub schema: String,
    pub tb: String,
    pub query: String,
    #[serde(skip)]
    pub meta: Option<StructModel>,
    #[serde(skip)]
    pub ddl_type: DdlType,
}

impl DdlData {
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        json!(self).to_string()
    }
}
