use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RouterConfig {
    pub db_map: String,
    pub tb_map: String,
    pub field_map: String,
}
