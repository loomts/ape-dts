use serde::{Deserialize, Serialize};

use crate::config::config_enums::DbType;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub enum DclStatement {
    CreateUser(OriginStatement),
    AlterUser(OriginStatement),
    CreateRole(OriginStatement),
    DropUser(OriginStatement),
    DropRole(OriginStatement),
    Grant(OriginStatement),
    Revoke(OriginStatement),
    SetRole(OriginStatement),

    #[default]
    Unknown,
}

impl DclStatement {
    pub fn get_schema_tb(&self) -> (String, String) {
        // Todo: support get schema and tb by dcl parser
        (String::new(), String::new())
    }

    pub fn route(&mut self, _dst_schema: String, _dst_tb: String) {
        // Todo: support route
    }

    pub fn to_sql(&self, _db_type: &DbType) -> String {
        match self {
            DclStatement::CreateUser(s)
            | DclStatement::AlterUser(s)
            | DclStatement::CreateRole(s)
            | DclStatement::DropUser(s)
            | DclStatement::DropRole(s)
            | DclStatement::SetRole(s)
            | DclStatement::Grant(s)
            | DclStatement::Revoke(s) => s.origin.clone(),

            DclStatement::Unknown => String::new(),
        }
    }

    pub fn get_malloc_size(&self) -> u64 {
        match self {
            DclStatement::CreateUser(s)
            | DclStatement::AlterUser(s)
            | DclStatement::CreateRole(s)
            | DclStatement::DropUser(s)
            | DclStatement::DropRole(s)
            | DclStatement::SetRole(s)
            | DclStatement::Grant(s)
            | DclStatement::Revoke(s) => s.origin.len() as u64,

            DclStatement::Unknown => 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct OriginStatement {
    pub origin: String,
}
