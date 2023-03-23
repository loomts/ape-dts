use strum::{Display, EnumString, IntoStaticStr};

#[derive(Clone, Display, EnumString, IntoStaticStr)]
pub enum DbType {
    #[strum(serialize = "mysql")]
    Mysql,
    #[strum(serialize = "pg")]
    Pg,
}

pub enum CharacterSetType {
    UTF8,
    GBK,
    UTF8MB4,
}
