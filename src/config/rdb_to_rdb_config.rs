use std::fs::File;

use std::io::prelude::*;

use serde::{Deserialize, Serialize};

use crate::error::Error;

use super::{filter_config::FilterConfig, route_config::RouteConfig};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RdbToRdbConfig {
    pub src_url: String,
    pub src_pool_size: u32,
    pub dst_url: String,
    pub dst_pool_size: u32,
    pub filter: FilterConfig,
    pub route: RouteConfig,
    pub buffer_size: usize,
}

impl RdbToRdbConfig {
    pub fn from_file(file_name: &str) -> Result<Self, Error> {
        let mut file = File::open(file_name)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config: Self = serde_yaml::from_str(&contents)?;
        Ok(config)
    }
}
