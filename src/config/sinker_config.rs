pub enum SinkerConfig {
    Mysql { url: String },

    Pg { url: String },
}
