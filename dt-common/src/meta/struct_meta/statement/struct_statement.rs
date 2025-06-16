use crate::rdb_filter::RdbFilter;

use super::{
    mysql_create_database_statement::MysqlCreateDatabaseStatement,
    mysql_create_table_statement::MysqlCreateTableStatement,
    pg_create_rbac_statement::PgCreateRbacStatement,
    pg_create_schema_statement::PgCreateSchemaStatement,
    pg_create_table_statement::PgCreateTableStatement,
};

#[derive(Debug, Clone, Default)]
pub enum StructStatement {
    MysqlCreateDatabase(MysqlCreateDatabaseStatement),
    PgCreateSchema(PgCreateSchemaStatement),
    MysqlCreateTable(MysqlCreateTableStatement),
    PgCreateTable(PgCreateTableStatement),
    PgCreateRbac(PgCreateRbacStatement),
    #[default]
    Unknown,
}

impl StructStatement {
    pub fn to_sqls(&mut self, filter: &RdbFilter) -> anyhow::Result<Vec<(String, String)>> {
        match self {
            Self::MysqlCreateDatabase(s) => s.to_sqls(filter),
            Self::PgCreateSchema(s) => s.to_sqls(filter),
            Self::MysqlCreateTable(s) => s.to_sqls(filter),
            Self::PgCreateTable(s) => s.to_sqls(filter),
            Self::PgCreateRbac(s) => s.to_sqls(filter),
            _ => Ok(vec![]),
        }
    }
}
