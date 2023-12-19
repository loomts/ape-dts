use dt_common::utils::rdb_filter::RdbFilter;

use crate::struct_meta::structure::{database::Database, structure_type::StructureType};

#[derive(Debug, Clone)]
pub struct PgCreateSchemaStatement {
    pub database: Database,
}

impl PgCreateSchemaStatement {
    pub fn to_sqls(&self, filter: &RdbFilter) -> Vec<(String, String)> {
        let mut sqls = Vec::new();
        if filter.filter_structure(StructureType::Database.into()) {
            return sqls;
        }

        let key = format!("schema.{}", self.database.name.clone());
        let sql = format!(r#"CREATE SCHEMA IF NOT EXISTS "{}""#, self.database.name);
        sqls.push((key, sql));
        sqls
    }
}
