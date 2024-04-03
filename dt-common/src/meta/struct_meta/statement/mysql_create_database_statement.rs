use crate::utils::rdb_filter::RdbFilter;

use crate::meta::struct_meta::structure::{database::Database, structure_type::StructureType};

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

        let mut sql = format!(r#"CREATE DATABASE IF NOT EXISTS `{}`"#, self.database.name);
        if !self.database.default_character_set_name.is_empty() {
            sql = format!(
                "{} DEFAULT CHARACTER SET {}",
                sql, self.database.default_character_set_name
            )
        }
        if !self.database.default_collation_name.is_empty() {
            sql = format!(
                "{} DEFAULT COLLATE {}",
                sql, self.database.default_collation_name
            )
        }

        let key = format!("database.{}", self.database.name.clone());
        sqls.push((key, sql));
        sqls
    }
}
