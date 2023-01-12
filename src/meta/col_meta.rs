use super::col_type::ColType;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ColMeta {
    pub name: String,
    pub typee: ColType,
}
