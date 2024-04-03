use crate::utils::rdb_filter::RdbFilter;

use super::{
    mysql_create_database_statement::MysqlCreateDatabaseStatement,
    mysql_create_table_statement::MysqlCreateTableStatement,
    pg_create_schema_statement::PgCreateSchemaStatement,
    pg_create_table_statement::PgCreateTableStatement,
};

#[derive(Debug, Clone)]
pub enum StructStatement {
    MysqlCreateDatabase {
        statement: MysqlCreateDatabaseStatement,
    },
    PgCreateSchema {
        statement: PgCreateSchemaStatement,
    },
    MysqlCreateTable {
        statement: MysqlCreateTableStatement,
    },
    PgCreateTable {
        statement: PgCreateTableStatement,
    },
}

impl StructStatement {
    pub fn to_sqls(&mut self, filter: &RdbFilter) -> Vec<(String, String)> {
        match self {
            Self::MysqlCreateDatabase { statement } => statement.to_sqls(filter),
            Self::PgCreateSchema { statement } => statement.to_sqls(filter),
            Self::MysqlCreateTable { statement } => statement.to_sqls(filter),
            Self::PgCreateTable { statement } => statement.to_sqls(filter),
        }
    }
}
