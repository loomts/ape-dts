use dt_common::utils::rdb_filter::RdbFilter;

use crate::struct_meta::structure::{database::Database, structure_type::StructureType};

#[derive(Debug, Clone)]
pub struct MysqlCreateDatabaseStatement {
    pub database: Database,
}

impl MysqlCreateDatabaseStatement {
    pub fn to_sqls(&self, filter: &RdbFilter) -> Vec<(String, String)> {
        let mut sqls = Vec::new();
        if filter.filter_structure(StructureType::Database.into()) {
            return sqls;
        }

        let key = format!("database.{}", self.database.name.clone());
        let sql = format!(r#"CREATE DATABASE IF NOT EXISTS `{}`"#, self.database.name);
        sqls.push((key, sql));
        sqls
    }
}
