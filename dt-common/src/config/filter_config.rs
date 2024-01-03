#[derive(Clone, Default)]
pub struct FilterConfig {
    pub do_dbs: String,
    pub ignore_dbs: String,
    pub do_tbs: String,
    pub ignore_tbs: String,
    pub do_events: String,
    pub do_structures: String,
    pub do_ddls: String,
}
