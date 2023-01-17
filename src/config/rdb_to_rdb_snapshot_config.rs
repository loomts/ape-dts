use serde::{Deserialize, Serialize};

use crate::error::Error;

use super::{filter_config::FilterConfig, router_config::RouterConfig};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RdbToRdbSnapshotConfig {
    pub src_url: String,
    pub src_pool_size: u32,
    pub dst_url: String,
    pub dst_pool_size: u32,
    pub filter: FilterConfig,
    pub router: RouterConfig,
    pub buffer_size: usize,
}

impl RdbToRdbSnapshotConfig {
    pub fn from_str(config: &str) -> Result<Self, Error> {
        Ok(serde_yaml::from_str(&config)?)
    }
}
