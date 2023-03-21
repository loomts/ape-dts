pub enum SinkerConfig {
    Mysql { url: String, batch_size: usize },

    Pg { url: String, batch_size: usize },

    MysqlCheck { url: String, batch_size: usize },

    PgCheck { url: String, batch_size: usize },
}
