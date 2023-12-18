use crate::struct_meta::structure::database::Database;

#[derive(Debug, Clone)]
pub struct MysqlCreateDatabaseStatement {
    pub database: Database,
}

impl MysqlCreateDatabaseStatement {
    pub fn to_sqls(&self) -> Vec<(String, String)> {
        let mut sqls = Vec::new();
        let key = format!("database.{}", self.database.name.clone());
        let sql = format!(r#"CREATE DATABASE IF NOT EXISTS `{}`"#, self.database.name);
        sqls.push((key, sql));
        sqls
    }
}
