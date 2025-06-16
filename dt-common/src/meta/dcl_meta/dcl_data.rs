use serde::{Deserialize, Serialize};
use serde_json::json;

use super::dcl_type::DclType;
use crate::config::config_enums::DbType;
use crate::meta::dcl_meta::dcl_statement::DclStatement;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DclData {
    pub default_schema: String,
    pub dcl_type: DclType,
    pub db_type: DbType,
    pub statement: DclStatement,
}

impl std::fmt::Display for DclData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

impl DclData {
    pub fn to_sql(&self) -> String {
        self.statement.to_sql(&self.db_type)
    }
}
