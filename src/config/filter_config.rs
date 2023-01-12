use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct FilterConfig {
    pub do_dbs: String,
    pub ignore_dbs: String,
    pub do_tbs: String,
    pub ignore_tbs: String,
    pub do_events: String,
}

impl FilterConfig {}
