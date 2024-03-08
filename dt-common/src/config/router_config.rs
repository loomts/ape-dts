#[derive(Clone)]
pub enum RouterConfig {
    Rdb {
        db_map: String,
        tb_map: String,
        col_map: String,
        topic_map: String,
    },
}
