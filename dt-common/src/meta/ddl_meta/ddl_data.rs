use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::config::config_enums::DbType;

use super::{ddl_statement::DdlStatement, ddl_type::DdlType};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DdlData {
    pub default_schema: String,
    pub query: String,
    pub ddl_type: DdlType,
    pub db_type: DbType,
    pub statement: DdlStatement,
}

impl std::fmt::Display for DdlData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

impl DdlData {
    pub fn to_sql(&self) -> String {
        self.statement.to_sql(&self.db_type)
    }

    pub fn get_schema_tb(&self) -> (String, String) {
        let (mut schema, tb) = self.statement.get_schema_tb();
        if schema.is_empty() {
            schema = self.default_schema.clone()
        }
        (schema, tb)
    }

    pub fn get_rename_to_schema_tb(&self) -> (String, String) {
        let (mut schema, tb) = self.statement.get_rename_to_schema_tb();
        if schema.is_empty() {
            schema = self.default_schema.clone()
        }
        (schema, tb)
    }

    pub fn split_to_multi(self) -> Vec<DdlData> {
        let mut res = Vec::new();
        for statement in self.statement.split_to_multi() {
            res.push(Self {
                default_schema: self.default_schema.clone(),
                query: self.query.clone(),
                ddl_type: self.ddl_type.clone(),
                db_type: self.db_type.clone(),
                statement,
            });
        }
        res
    }
}
