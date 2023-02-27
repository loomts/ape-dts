use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PgColType {
    pub name: String,
    pub oid: i32,
    pub parent_oid: i32,
    pub element_oid: i32,
    pub modifiers: i32,
    pub category: String,
    pub enum_values: String,
}

const CATEGORY_ENUM: &str = "E";
const CATEGORY_ARRAY: &str = "A";

impl PgColType {
    pub fn is_enum(&self) -> bool {
        CATEGORY_ENUM == self.category
    }

    pub fn is_array(&self) -> bool {
        CATEGORY_ARRAY == self.category
    }
}
