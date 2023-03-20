use crate::meta::db_enum::DatabaseType;

#[derive(Clone)]
pub enum SinkerConfig {
    BasicStruct { url: String, db_type: DatabaseType },

    Mysql { url: String },

    Pg { url: String },
}
