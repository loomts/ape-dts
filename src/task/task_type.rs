pub enum TaskType {
    Unkown,
    MysqlToMysqlSnapshot,
    MysqlToMysqlCdc,
}

impl TaskType {
    pub fn from_name(name: &str) -> Self {
        match name {
            "mysql_to_mysql_snapshot" => Self::MysqlToMysqlSnapshot,
            "mysql_to_mysql_cdc" => Self::MysqlToMysqlCdc,
            _ => Self::Unkown,
        }
    }
}
