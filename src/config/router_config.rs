pub enum RouterConfig {
    Rdb {
        db_map: String,
        tb_map: String,
        field_map: String,
    },
}
