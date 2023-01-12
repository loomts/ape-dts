use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RouteConfig {
    pub db_map: String,
    pub tb_map: String,
    pub field_map: String,
}
