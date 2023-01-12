use std::collections::HashMap;

use crate::{config::route_config::RouteConfig, error::Error};

#[derive(Debug, Clone)]
pub struct Router {
    pub db_map: HashMap<String, String>,
    pub tb_map: HashMap<String, String>,
    pub cache: HashMap<String, (String, String)>,
}

impl Router {
    pub fn from_config(config: &RouteConfig) -> Result<Self, Error> {
        let db_map = Self::parse_str(&config.db_map);
        let tb_map = Self::parse_str(&config.tb_map);
        Ok(Self {
            db_map,
            tb_map,
            cache: HashMap::new(),
        })
    }

    pub fn get_route(&mut self, db: &str, tb: &str) -> (String, String) {
        let full_name = format!("{}.{}", db, tb);
        if let Some(result) = self.cache.get(&full_name) {
            return result.clone();
        }

        if let Some(map) = self.tb_map.get(&full_name) {
            let vec = map.split(".").collect::<Vec<&str>>();
            let result = (
                vec.get(0).unwrap().to_string(),
                vec.get(1).unwrap().to_string(),
            );
            self.cache.insert(full_name, result.clone());
            return result;
        }

        return (db.to_string(), tb.to_string());
    }

    fn parse_str(str: &str) -> HashMap<String, String> {
        let mut map = HashMap::new();
        if str.is_empty() {
            return map;
        }

        for i in str.split(",") {
            let vec = i.split(":").collect::<Vec<&str>>();
            if vec.len() == 2 {
                map.insert(
                    vec.get(0).unwrap().to_string(),
                    vec.get(1).unwrap().to_string(),
                );
            }
        }
        map
    }
}
