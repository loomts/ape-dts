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
    PgCreateDatabase {
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
    pub fn to_sqls(&mut self) -> Vec<(String, String)> {
        match self {
            Self::MysqlCreateDatabase { statement } => statement.to_sqls(),
            Self::PgCreateDatabase { statement } => statement.to_sqls(),
            Self::MysqlCreateTable { statement } => statement.to_sqls(),
            Self::PgCreateTable { statement } => statement.to_sqls(),
        }
    }
}
