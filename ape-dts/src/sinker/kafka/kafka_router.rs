use std::collections::HashMap;

use dt_common::config::router_config::RouterConfig;

use crate::error::Error;

#[derive(Debug, Clone)]
pub struct KafkaRouter {
    pub db_map: HashMap<String, String>,
    pub tb_map: HashMap<String, String>,
    pub cache: HashMap<String, String>,
}

impl KafkaRouter {
    pub fn from_config(config: &RouterConfig) -> Result<Self, Error> {
        match config {
            RouterConfig::Rdb { db_map, tb_map, .. } => Ok(Self {
                db_map: Self::parse_str(db_map)?,
                tb_map: Self::parse_str(tb_map)?,
                cache: HashMap::new(),
            }),
        }
    }

    pub fn get_route(&mut self, db: &str, tb: &str) -> String {
        let full_name = format!("{}.{}", db, tb);
        if let Some(topic) = self.cache.get(&full_name) {
            return topic.clone();
        }

        if let Some(topic) = self.tb_map.get(&full_name) {
            self.cache.insert(full_name, topic.to_string());
            return topic.to_string();
        }

        if let Some(topic) = self.db_map.get(db) {
            self.cache.insert(full_name, topic.to_string());
            return topic.to_string();
        }

        return self.db_map.get("*").unwrap().to_string();
    }

    fn parse_str(config_str: &str) -> Result<HashMap<String, String>, Error> {
        let mut map = HashMap::new();
        if config_str.is_empty() {
            return Ok(map);
        }

        for name in config_str.split(",") {
            let tokens: Vec<&str> = name.split(":").collect();

            if tokens.len() != 2 {
                return Err(Error::ConfigError {
                    error: format!("invalid router config, check error near: {}", name),
                });
            }
            map.insert(
                tokens.get(0).unwrap().to_string(),
                tokens.get(1).unwrap().to_string(),
            );
        }
        Ok(map)
    }
}
