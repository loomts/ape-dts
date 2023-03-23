use strum::EnumString;

#[derive(Clone, EnumString)]
pub enum DatabaseType {
    MySQL,
    PostgresSQL,
}

pub enum CharacterSetType {
    UTF8,
    GBK,
    UTF8MB4,
}
