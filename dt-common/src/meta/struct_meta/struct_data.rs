use serde::{Deserialize, Serialize};
use serde_json::json;

use super::statement::struct_statement::StructStatement;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructData {
    pub db: String,
    #[serde(skip)]
    pub statement: StructStatement,
}

impl std::fmt::Display for StructData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}
