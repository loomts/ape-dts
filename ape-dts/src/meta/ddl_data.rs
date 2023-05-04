use serde::{Deserialize, Serialize};
use serde_json::json;

use super::ddl_type::DdlType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DdlData {
    pub schema: String,
    pub query: String,
    #[serde(skip)]
    pub ddl_type: DdlType,
}

impl DdlData {
    pub fn to_string(&self) -> String {
        json!(self).to_string()
    }
}
