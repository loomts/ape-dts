#[derive(Clone)]
pub enum FilterConfig {
    Rdb {
        do_dbs: String,
        ignore_dbs: String,
        do_tbs: String,
        ignore_tbs: String,
        do_events: String,
        do_structures: String,
    },
}
