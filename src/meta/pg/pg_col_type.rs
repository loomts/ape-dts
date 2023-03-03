use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PgColType {
    pub long_name: String,
    pub short_name: String,
    pub oid: i32,
    pub parent_oid: i32,
    pub element_oid: i32,
    pub modifiers: i32,
    pub category: String,
    pub enum_values: Option<Vec<String>>,
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

    pub fn get_short_type_name(full_type_name: &str) -> String {
        let short_type_name = match full_type_name {
            "bigint" => "int8",
            "boolean" => "bool",
            "character" => "bpchar",
            "character varying" => "varchar",
            "double precision" => "float8",
            "integer" => "int4",
            "real" => "float4",
            "smallint" => "int2",
            "bit varying" => "varbit",
            "timestamp with time zone" => "timestamptz",
            "timestamp without time zone" => "timestamp",
            "time without time zone" => "time",
            "time with time zone" => "timetz",
            _ => full_type_name,
        };
        short_type_name.to_string()
    }
}
