use crate::struct_meta::structure::database::Database;

#[derive(Debug, Clone)]
pub struct PgCreateSchemaStatement {
    pub database: Database,
}

impl PgCreateSchemaStatement {
    pub fn to_sqls(&self) -> Vec<(String, String)> {
        let mut sqls = Vec::new();
        let key = format!("schema.{}", self.database.name.clone());
        let sql = format!(r#"CREATE SCHEMA IF NOT EXISTS "{}""#, self.database.name);
        sqls.push((key, sql));
        sqls
    }
}
