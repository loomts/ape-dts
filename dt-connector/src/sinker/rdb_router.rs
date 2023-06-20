use std::collections::HashMap;

use dt_common::{
    config::{config_enums::RouteType, router_config::RouterConfig},
    error::Error,
};
use regex::Regex;

#[derive(Debug, Clone)]
pub struct RdbRouter {
    pub db_map: HashMap<String, String>,
    pub tb_map: HashMap<String, String>,
    pub cache: HashMap<String, (String, String)>,
}

impl RdbRouter {
    pub fn from_config(config: &RouterConfig) -> Result<Self, Error> {
        match config {
            RouterConfig::Rdb { db_map, tb_map, .. } => Ok(Self {
                db_map: Self::parse_str(db_map, RouteType::Db)?,
                tb_map: Self::parse_str(tb_map, RouteType::Tb)?,
                cache: HashMap::new(),
            }),
        }
    }

    pub fn get_route(&mut self, db: &str, tb: &str) -> (String, String) {
        let full_name = format!("{}.{}", db, tb);
        if let Some(result) = self.cache.get(&full_name) {
            return result.clone();
        }

        if let Some(map) = self.tb_map.get(&full_name) {
            let tokens = map.split('.').collect::<Vec<&str>>();
            let result = (
                tokens.first().unwrap().to_string(),
                tokens.get(1).unwrap().to_string(),
            );
            self.cache.insert(full_name, result.clone());
            return result;
        }

        if let Some(map) = self.db_map.get(db) {
            let result = (map.clone(), tb.to_string());
            self.cache.insert(full_name, result.clone());
            return result;
        }

        (db.to_string(), tb.to_string())
    }

    fn parse_str(config_str: &str, name_type: RouteType) -> Result<HashMap<String, String>, Error> {
        let mut map = HashMap::new();
        if config_str.is_empty() {
            return Ok(map);
        }

        for name in config_str.split(',') {
            let tokens: Vec<&str> = name.split(':').collect();

            if tokens.len() != 2
                || !Self::is_valid_name(tokens[0], &name_type)
                || !Self::is_valid_name(tokens[1], &name_type)
            {
                return Err(Error::ConfigError {
                    error: format!("invalid router config, check error near: {}", name),
                });
            }
            map.insert(
                tokens.first().unwrap().to_string(),
                tokens.get(1).unwrap().to_string(),
            );
        }
        Ok(map)
    }

    fn is_valid_name(name: &str, name_type: &RouteType) -> bool {
        let re = Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]{0,63}$").unwrap();
        match name_type {
            RouteType::Db => re.is_match(name),

            RouteType::Tb => {
                let tokens: Vec<&str> = name.split('.').collect();
                if tokens.len() != 2 {
                    return false;
                }
                re.is_match(tokens[0]) & re.is_match(tokens[1])
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_valid_db_names() {
        assert!(RdbRouter::is_valid_name("my_database", &RouteType::Db));
        assert!(RdbRouter::is_valid_name("database1", &RouteType::Db));
        assert!(RdbRouter::is_valid_name("_database", &RouteType::Db));
        assert!(RdbRouter::is_valid_name("a", &RouteType::Db));
    }

    #[test]
    fn test_invalid_db_names() {
        // empty
        assert!(!RdbRouter::is_valid_name("", &RouteType::Db));
        // invalid character
        assert!(!RdbRouter::is_valid_name("database*", &RouteType::Db));
        // too long
        assert!(!RdbRouter::is_valid_name(
            "ttttttttttttttttttttttttttttttttttttttt_this_is_a_really_long_database_name_that_is_over_64_characters",
            &RouteType::Db
        ));
    }

    #[test]
    fn test_valid_dbtb_names() {
        assert!(RdbRouter::is_valid_name("my_database.tb_1", &RouteType::Tb));
        assert!(RdbRouter::is_valid_name("_database.tb_1", &RouteType::Tb));
        assert!(RdbRouter::is_valid_name("a._tb_2", &RouteType::Tb));
        assert!(RdbRouter::is_valid_name(
            "a123456789012345678901234567890123456789012345678901234567890123.a123456789012345678901234567890123456789012345678901234567890123",
            &RouteType::Tb
        ));
    }

    #[test]
    fn test_invalid_dbtb_names() {
        // only db
        assert!(!RdbRouter::is_valid_name("my_database", &RouteType::Tb));
        // empty tb
        assert!(!RdbRouter::is_valid_name("my_database.", &RouteType::Tb));
        // emtpy tb
        assert!(!RdbRouter::is_valid_name(".my_database", &RouteType::Tb));
        // more than 2 parts
        assert!(!RdbRouter::is_valid_name("_database.tb_1.", &RouteType::Tb));
        // invalid characters in tb
        assert!(!RdbRouter::is_valid_name("a.-database", &RouteType::Tb));
        assert!(!RdbRouter::is_valid_name("*.*", &RouteType::Tb));
        assert!(!RdbRouter::is_valid_name("*.?", &RouteType::Tb));
        assert!(!RdbRouter::is_valid_name("a*b?c.a*b?c", &RouteType::Tb));
        // tb too long
        assert!(!RdbRouter::is_valid_name(
            "a.ttttttttttttttttttttttttttttttttttttttt_this_is_a_really_long_database_name_that_is_over_64_characters",
            &RouteType::Tb
        ));
    }

    #[test]
    fn test_rdb_router_parse_str_ok() {
        let mut result = HashMap::new();
        result.insert("a".to_string(), "b".to_string());
        result.insert("c".to_string(), "d".to_string());
        assert_eq!(
            RdbRouter::parse_str("a:b,c:d", RouteType::Db).unwrap(),
            result
        );

        result.clear();
        result.insert("a.a".to_string(), "b.b".to_string());
        result.insert("c.c".to_string(), "d.d".to_string());
        assert_eq!(
            RdbRouter::parse_str("a.a:b.b,c.c:d.d", RouteType::Tb).unwrap(),
            result
        );
    }

    #[test]
    fn test_rdb_router_parse_str_err() {
        // miss map value
        assert!(RdbRouter::parse_str("a", RouteType::Db).is_err());
        // mis map value
        assert!(RdbRouter::parse_str("a:b,c", RouteType::Db).is_err());
        // invalid characters in name
        assert!(RdbRouter::parse_str("a:b,c&:d", RouteType::Db).is_err());
        // wrong map
        assert!(RdbRouter::parse_str("a:b:b,c:d", RouteType::Db).is_err());

        // miss map value
        assert!(RdbRouter::parse_str("a.b", RouteType::Tb).is_err());
        // mis map value
        assert!(RdbRouter::parse_str("a.a:b.b,c.c", RouteType::Tb).is_err());
        // invalid characters in name
        assert!(RdbRouter::parse_str("a.a:b.b,c&.c:d.d", RouteType::Tb).is_err());
        // wrong map
        assert!(RdbRouter::parse_str("a.a:b.b:c.c,c.c:d.d", RouteType::Tb).is_err());
    }

    #[test]
    fn test_rdb_router() {
        let config = RouterConfig::Rdb {
            db_map: "a:b,c:d".to_string(),
            tb_map: "a.a:b.b,c.c:d.d".to_string(),
            field_map: String::new(),
        };
        let mut router = RdbRouter::from_config(&config).unwrap();
        // hit tb_map
        assert_eq!(
            router.get_route("a", "a"),
            ("b".to_string(), "b".to_string())
        );
        // hit db_map
        assert_eq!(
            router.get_route("a", "c"),
            ("b".to_string(), "c".to_string())
        );
        // no hit
        assert_eq!(
            router.get_route("d", "d"),
            ("d".to_string(), "d".to_string())
        );
    }
}
