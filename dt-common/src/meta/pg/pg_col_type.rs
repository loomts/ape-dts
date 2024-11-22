use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PgColType {
    pub name: String,
    pub alias: String,
    pub oid: i32,
    pub parent_oid: i32,
    pub element_oid: i32,
    pub modifiers: i32,
    pub category: String,
    pub enum_values: Option<Vec<String>>,
}

impl std::fmt::Display for PgColType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

#[allow(dead_code)]
impl PgColType {
    pub fn is_enum(&self) -> bool {
        "E" == self.category
    }

    pub fn is_array(&self) -> bool {
        "A" == self.category
    }

    pub fn is_user_defined(&self) -> bool {
        "U" == self.category
    }

    pub fn get_alias(name: &str) -> String {
        // refer to: https://www.postgresql.org/docs/17/datatype.html
        match name {
            "bigint" => "int8",
            "bigserial" => "serial8",
            "bit varying" => "varbit",
            "boolean" => "bool",
            // fixed-length, blank-padded, refer to: https://www.postgresql.org/docs/17/datatype-character.html
            "character" | "char" => "bpchar",
            "character varying" => "varchar",
            "double precision" => "float8",
            "int" | "integer" => "int4",
            "numeric" => "decimal",
            "real" => "float4",
            "smallint" => "int2",
            "smallserial" => "serial2",
            "serial" => "serial4",
            "timestamp with time zone" => "timestamptz",
            "timestamp without time zone" => "timestamp",
            "time without time zone" => "time",
            "time with time zone" => "timetz",
            _ => name,
        }
        .to_string()
    }
}
