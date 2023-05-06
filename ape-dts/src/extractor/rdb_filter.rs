use std::collections::{HashMap, HashSet};

use dt_common::config::filter_config::FilterConfig;
use strum::AsStaticRef;

use crate::{error::Error, meta::row_type::RowType};

use regex::Regex;

#[derive(Debug, Clone)]
pub struct RdbFilter {
    pub do_dbs: HashSet<String>,
    pub ignore_dbs: HashSet<String>,
    pub do_tbs: HashSet<String>,
    pub ignore_tbs: HashSet<String>,
    pub do_events: HashSet<String>,
    pub cache: HashMap<String, bool>,
}

impl RdbFilter {
    pub fn from_config(config: &FilterConfig) -> Result<Self, Error> {
        match config {
            FilterConfig::Rdb {
                do_dbs,
                ignore_dbs,
                do_tbs,
                ignore_tbs,
                do_events,
            } => Ok(Self {
                do_dbs: Self::parse_str(do_dbs, NameType::Db)?,
                ignore_dbs: Self::parse_str(ignore_dbs, NameType::Db)?,
                do_tbs: Self::parse_str(do_tbs, NameType::DbTb)?,
                ignore_tbs: Self::parse_str(ignore_tbs, NameType::DbTb)?,
                do_events: Self::parse_str(do_events, NameType::Event)?,
                cache: HashMap::new(),
            }),
        }
    }

    pub fn filter_db(&mut self, db: &str) -> bool {
        let full_name = format!("{}.*", db);
        let filter =
            Self::contain(&self.ignore_tbs, &full_name) || Self::contain(&self.ignore_dbs, db);

        let mut keep = Self::contain(&self.do_dbs, db);
        if !filter && !keep {
            for do_tb in self.do_tbs.iter() {
                let tokens: Vec<&str> = do_tb.split(".").collect();
                let db_in_do_tb = tokens[0];
                if Self::match_name(db_in_do_tb, db) {
                    keep = true;
                    break;
                }
            }
        }

        filter || !keep
    }

    pub fn filter(&mut self, db: &str, tb: &str, row_type: &RowType) -> bool {
        if self.do_events.is_empty() || !self.do_events.contains(row_type.as_static()) {
            return false;
        }

        let full_name = format!("{}.{}", db, tb);
        if let Some(cache) = self.cache.get(&full_name) {
            return *cache;
        }

        let filter =
            Self::contain(&self.ignore_tbs, &full_name) || Self::contain(&self.ignore_dbs, db);
        let keep = Self::contain(&self.do_tbs, &full_name) || Self::contain(&self.do_dbs, db);
        let filter = filter || !keep;
        self.cache.insert(full_name, filter);
        filter
    }

    fn contain(set: &HashSet<String>, item: &str) -> bool {
        for i in set.iter() {
            if Self::match_name(i, item) {
                return true;
            }
        }
        false
    }

    fn match_name(pattern: &str, item: &str) -> bool {
        let mut pattern = pattern
            .replace(".", "\\.")
            .replace("*", ".*")
            .replace("?", ".?");
        pattern = format!(r"^{}$", pattern);
        if Regex::new(&pattern).unwrap().is_match(item) {
            return true;
        }
        false
    }

    fn parse_str(config_str: &str, name_type: NameType) -> Result<HashSet<String>, Error> {
        let mut set = HashSet::new();
        if config_str.is_empty() {
            return Ok(set);
        }

        for name in config_str.split(",") {
            if !Self::is_valid_name(name, &name_type) {
                return Err(Error::ConfigError {
                    error: format!("invalid filter config, check error near: {}", name),
                });
            }
            set.insert(name.to_string());
        }
        Ok(set)
    }

    fn is_valid_name(name: &str, name_type: &NameType) -> bool {
        let re = Regex::new(r"^[a-zA-Z0-9_\?\*]{1,64}$").unwrap();
        match name_type {
            NameType::Db => re.is_match(name),

            NameType::DbTb => {
                let tokens: Vec<&str> = name.split(".").collect();
                if tokens.len() != 2 {
                    return false;
                }
                re.is_match(tokens[0]) & re.is_match(tokens[1])
            }

            NameType::Event => true,
        }
    }
}

enum NameType {
    Db,
    DbTb,
    Event,
}

#[cfg(test)]
mod tests {
    use strum::AsStaticRef;

    use super::*;

    #[test]
    fn test_match_name_exact_match() {
        assert!(RdbFilter::match_name("hello", "hello"));
        assert!(!RdbFilter::match_name("hello", "hellO"));
    }

    #[test]
    fn test_match_name_question_mark() {
        assert!(RdbFilter::match_name("he?lo", "hello"));
        assert!(RdbFilter::match_name("he?lo", "helo"));
        assert!(!RdbFilter::match_name("he?lo", "helllo"));
    }

    #[test]
    fn test_match_name_asterisk() {
        assert!(RdbFilter::match_name("he*llo", "hello"));
        assert!(RdbFilter::match_name("he*llo", "heeeeeello"));
        assert!(RdbFilter::match_name("he*llo", "hello"));
        assert!(!RdbFilter::match_name("he*llo", "helo"));
    }

    #[test]
    fn test_match_name_dot() {
        assert!(RdbFilter::match_name("h.llo", "h.llo"));
        assert!(!RdbFilter::match_name("h.llo", "he.llo"));
        assert!(!RdbFilter::match_name("h.llo", "h.lo"));
        assert!(!RdbFilter::match_name("h.llo", "hello"));
    }

    #[test]
    fn test_valid_db_names() {
        assert!(RdbFilter::is_valid_name("my_database", &NameType::Db));
        assert!(RdbFilter::is_valid_name("database1", &NameType::Db));
        assert!(RdbFilter::is_valid_name("_database", &NameType::Db));
        assert!(RdbFilter::is_valid_name("a", &NameType::Db));
        assert!(RdbFilter::is_valid_name("*", &NameType::Db));
        assert!(RdbFilter::is_valid_name("?", &NameType::Db));
        assert!(RdbFilter::is_valid_name("*?", &NameType::Db));
        assert!(RdbFilter::is_valid_name("a*b?c", &NameType::Db));
    }

    #[test]
    fn test_invalid_db_names() {
        // empty
        assert!(!RdbFilter::is_valid_name("", &NameType::Db));
        // invalid characters
        assert!(!RdbFilter::is_valid_name("database@", &NameType::Db));
        // too long
        assert!(!RdbFilter::is_valid_name(
            "ttttttttttttttttttttttttttttttttttttttt_this_is_a_really_long_database_name_that_is_over_64_characters",
            &NameType::Db
        ));
    }

    #[test]
    fn test_valid_dbtb_names() {
        assert!(RdbFilter::is_valid_name(
            "my_database.tb_1",
            &NameType::DbTb
        ));
        assert!(RdbFilter::is_valid_name("_database.tb_1", &NameType::DbTb));
        assert!(RdbFilter::is_valid_name("a._tb_2", &NameType::DbTb));
        assert!(RdbFilter::is_valid_name(
            "a123456789012345678901234567890123456789012345678901234567890123.a123456789012345678901234567890123456789012345678901234567890123",
            &NameType::DbTb
        ));
        assert!(RdbFilter::is_valid_name("*.*", &NameType::DbTb));
        assert!(RdbFilter::is_valid_name("*.?", &NameType::DbTb));
        assert!(RdbFilter::is_valid_name("a*b?c.a*b?c", &NameType::DbTb));
    }

    #[test]
    fn test_invalid_dbtb_names() {
        // only db
        assert!(!RdbFilter::is_valid_name("my_database", &NameType::DbTb));
        // empty tb
        assert!(!RdbFilter::is_valid_name("my_database.", &NameType::DbTb));
        // emtpy tb
        assert!(!RdbFilter::is_valid_name(".my_database", &NameType::DbTb));
        // more than 2 parts
        assert!(!RdbFilter::is_valid_name(
            "_database.tb_1.",
            &NameType::DbTb
        ));
        // invalid characters in tb
        assert!(!RdbFilter::is_valid_name("a.-database", &NameType::DbTb));
        // tb too long
        assert!(!RdbFilter::is_valid_name(
            "a.ttttttttttttttttttttttttttttttttttttttt_this_is_a_really_long_database_name_that_is_over_64_characters",
            &NameType::DbTb
        ));
    }

    #[test]
    fn test_rdb_filter_ignore_tbs() {
        let config = FilterConfig::Rdb {
            do_dbs: "*".to_string(),
            ignore_dbs: String::new(),
            do_tbs: "*.*".to_string(),
            ignore_tbs: "*.b*".to_string(),
            do_events: RowType::Insert.as_static().to_string(),
        };
        let mut rdb_fitler = RdbFilter::from_config(&config).unwrap();
        assert!(rdb_fitler.filter("a", "bcd", &RowType::Insert));
        assert!(rdb_fitler.filter("b", "b", &RowType::Insert));
        assert!(!rdb_fitler.filter("a", "cbd", &RowType::Insert));
        assert!(!rdb_fitler.filter("b", "cbd", &RowType::Insert));
    }

    #[test]
    fn test_rdb_filter_ignore_dbs() {
        let config = FilterConfig::Rdb {
            do_dbs: "*".to_string(),
            ignore_dbs: "a*".to_string(),
            do_tbs: "*.*".to_string(),
            ignore_tbs: String::new(),
            do_events: RowType::Insert.as_static().to_string(),
        };
        let mut rdb_fitler = RdbFilter::from_config(&config).unwrap();
        assert!(rdb_fitler.filter("abc", "bcd", &RowType::Insert));
        assert!(rdb_fitler.filter("a", "bcd", &RowType::Insert));
        assert!(!rdb_fitler.filter("b", "cbd", &RowType::Insert));
        assert!(!rdb_fitler.filter("b", "cbd", &RowType::Insert));
    }

    #[test]
    fn test_rdb_filter_do_dbs() {
        let config = FilterConfig::Rdb {
            do_dbs: "b*".to_string(),
            ignore_dbs: "a*".to_string(),
            do_tbs: "aaaaaaa.*".to_string(),
            ignore_tbs: String::new(),
            do_events: RowType::Insert.as_static().to_string(),
        };
        let mut rdb_fitler = RdbFilter::from_config(&config).unwrap();
        assert!(rdb_fitler.filter("a", "bcd", &RowType::Insert));
        assert!(rdb_fitler.filter("c", "bcd", &RowType::Insert));
        assert!(!rdb_fitler.filter("b", "bcd", &RowType::Insert));
        assert!(!rdb_fitler.filter("bcd", "bcd", &RowType::Insert));
    }

    #[test]
    fn test_rdb_filter_do_tbs() {
        let config = FilterConfig::Rdb {
            do_dbs: String::new(),
            ignore_dbs: "b*".to_string(),
            do_tbs: "a*.*".to_string(),
            ignore_tbs: String::new(),
            do_events: RowType::Insert.as_static().to_string(),
        };
        let mut rdb_fitler = RdbFilter::from_config(&config).unwrap();
        assert!(rdb_fitler.filter("bcd", "bcd", &RowType::Insert));
        assert!(rdb_fitler.filter("cde", "bcd", &RowType::Insert));
        assert!(!rdb_fitler.filter("a", "bcd", &RowType::Insert));
        assert!(!rdb_fitler.filter("abc", "bcd", &RowType::Insert));
    }

    #[test]
    fn test_rdb_filter_db() {
        // keep by do_dbs, Not filtered by ignore_dbs
        let config = FilterConfig::Rdb {
            do_dbs: "test_db_*".to_string(),
            ignore_dbs: "test_db_2".to_string(),
            do_tbs: String::new(),
            ignore_tbs: String::new(),
            do_events: RowType::Insert.as_static().to_string(),
        };
        let mut rdb_fitler = RdbFilter::from_config(&config).unwrap();
        assert!(!rdb_fitler.filter_db("test_db_1"));

        // keep by do_dbs, filtered by ignore_dbs exactly
        let config = FilterConfig::Rdb {
            do_dbs: "test_db_*".to_string(),
            ignore_dbs: "test_db_1".to_string(),
            do_tbs: String::new(),
            ignore_tbs: String::new(),
            do_events: RowType::Insert.as_static().to_string(),
        };
        let mut rdb_fitler = RdbFilter::from_config(&config).unwrap();
        assert!(rdb_fitler.filter_db("test_db_1"));

        // keep by do_dbs, filtered by ignore_dbs wildchar
        let config = FilterConfig::Rdb {
            do_dbs: "test_db_1".to_string(),
            ignore_dbs: "*".to_string(),
            do_tbs: String::new(),
            ignore_tbs: String::new(),
            do_events: RowType::Insert.as_static().to_string(),
        };
        let mut rdb_fitler = RdbFilter::from_config(&config).unwrap();
        assert!(rdb_fitler.filter_db("test_db_1"));

        // keep by do_dbs, NOT all tables filtered by ignore_tbs
        let config = FilterConfig::Rdb {
            do_dbs: "test_db_*".to_string(),
            ignore_dbs: String::new(),
            do_tbs: String::new(),
            ignore_tbs: "test_db_1.a*".to_string(),
            do_events: RowType::Insert.as_static().to_string(),
        };
        let mut rdb_fitler = RdbFilter::from_config(&config).unwrap();
        assert!(!rdb_fitler.filter_db("test_db_1"));

        // keep by do_dbs, all tables filtered by ignore_tbs
        let config = FilterConfig::Rdb {
            do_dbs: "test_db_*".to_string(),
            ignore_dbs: String::new(),
            do_tbs: String::new(),
            ignore_tbs: "test_db_1.*".to_string(),
            do_events: RowType::Insert.as_static().to_string(),
        };
        let mut rdb_fitler = RdbFilter::from_config(&config).unwrap();
        assert!(rdb_fitler.filter_db("test_db_1"));

        // keep by do_tbs, NOT all tables filtered by ignore_tbs
        let config = FilterConfig::Rdb {
            do_dbs: String::new(),
            ignore_dbs: String::new(),
            do_tbs: "test_db_1.one_pk_multi_uk".to_string(),
            ignore_tbs: "test_db_*.a*".to_string(),
            do_events: RowType::Insert.as_static().to_string(),
        };
        let mut rdb_fitler = RdbFilter::from_config(&config).unwrap();
        assert!(!rdb_fitler.filter_db("test_db_1"));

        // keep by do_tbs, all tables filtered by ignore_tbs
        let config = FilterConfig::Rdb {
            do_dbs: String::new(),
            ignore_dbs: "b*".to_string(),
            do_tbs: "test_db_1.one_pk_multi_uk".to_string(),
            ignore_tbs: "test_db_*.*".to_string(),
            do_events: RowType::Insert.as_static().to_string(),
        };
        let mut rdb_fitler = RdbFilter::from_config(&config).unwrap();
        assert!(rdb_fitler.filter_db("test_db_1"));

        // keep by do_tbs, NOT filtered by ignore_dbs
        let config = FilterConfig::Rdb {
            do_dbs: String::new(),
            ignore_dbs: "test_db_2".to_string(),
            do_tbs: "test_db_1.one_pk_multi_uk".to_string(),
            ignore_tbs: String::new(),
            do_events: RowType::Insert.as_static().to_string(),
        };
        let mut rdb_fitler = RdbFilter::from_config(&config).unwrap();
        assert!(!rdb_fitler.filter_db("test_db_1"));

        // keep by do_tbs, filtered by ignore_dbs exactly
        let config = FilterConfig::Rdb {
            do_dbs: String::new(),
            ignore_dbs: "test_db_1".to_string(),
            do_tbs: "test_db_1.one_pk_multi_uk".to_string(),
            ignore_tbs: String::new(),
            do_events: RowType::Insert.as_static().to_string(),
        };
        let mut rdb_fitler = RdbFilter::from_config(&config).unwrap();
        assert!(rdb_fitler.filter_db("test_db_1"));

        // keep by do_tbs, filtered by ignore_dbs wildchar
        let config = FilterConfig::Rdb {
            do_dbs: String::new(),
            ignore_dbs: "test_db_*".to_string(),
            do_tbs: "test_db_1.one_pk_multi_uk".to_string(),
            ignore_tbs: String::new(),
            do_events: RowType::Insert.as_static().to_string(),
        };
        let mut rdb_fitler = RdbFilter::from_config(&config).unwrap();
        assert!(rdb_fitler.filter_db("test_db_1"));
    }
}
