use std::collections::{HashMap, HashSet};

use crate::{config::filter_config::FilterConfig, error::Error, meta::row_type::RowType};

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
                do_dbs: Self::parse_str(do_dbs),
                ignore_dbs: Self::parse_str(ignore_dbs),
                do_tbs: Self::parse_str(do_tbs),
                ignore_tbs: Self::parse_str(ignore_tbs),
                do_events: Self::parse_str(do_events),
                cache: HashMap::new(),
            }),
        }
    }

    pub fn filter(&mut self, db: &str, tb: &str, row_type: &RowType) -> bool {
        if !self.do_events.is_empty() && !self.do_events.contains(row_type.to_str()) {
            return true;
        }

        let full_name = format!("{}.{}", db, tb);
        if let Some(cache) = self.cache.get(&full_name) {
            return *cache;
        }

        let result = !self.tb_ok(&full_name) || !self.db_ok(db);
        self.cache.insert(full_name, result);
        result
    }

    fn tb_ok(&self, full_name: &str) -> bool {
        if !self.do_tbs.is_empty() && self.do_tbs.contains(full_name) {
            return true;
        }

        if !self.ignore_tbs.is_empty() && self.ignore_tbs.contains(full_name) {
            return false;
        }

        self.do_tbs.is_empty()
    }

    fn db_ok(&self, db: &str) -> bool {
        if !self.do_dbs.is_empty() {
            return self.do_dbs.contains(db);
        }

        if !self.ignore_dbs.is_empty() {
            return !self.ignore_dbs.contains(db);
        }

        true
    }

    fn parse_str(str: &str) -> HashSet<String> {
        let mut set = HashSet::new();
        if !str.is_empty() {
            for i in str.split(",") {
                set.insert(i.to_string());
            }
        }
        set
    }
}
