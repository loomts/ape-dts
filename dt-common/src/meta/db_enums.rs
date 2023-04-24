use strum::{Display, EnumString, IntoStaticStr};

#[derive(Clone, Display, EnumString, IntoStaticStr)]
pub enum DbType {
    #[strum(serialize = "mysql")]
    Mysql,
    #[strum(serialize = "pg")]
    Pg,
    #[strum(serialize = "kafka")]
    Kafka,
    #[strum(serialize = "open_faas")]
    OpenFaas,
    #[strum(serialize = "foxlake")]
    Foxlake,
}

pub enum CharacterSetType {
    UTF8,
    GBK,
    UTF8MB4,
}
