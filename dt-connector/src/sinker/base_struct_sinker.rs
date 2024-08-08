use anyhow::bail;
use dt_common::log_error;
use dt_common::meta::struct_meta::statement::struct_statement::StructStatement;
use dt_common::{
    config::config_enums::ConflictPolicyEnum, error::Error, log_info, rdb_filter::RdbFilter,
};
use sqlx::{query, MySql, Pool, Postgres};

use crate::rdb_router::RdbRouter;

pub struct BaseStructSinker {}

pub enum DBConnPool {
    MySQL(Pool<MySql>),
    PostgreSQL(Pool<Postgres>),
}

impl BaseStructSinker {
    pub fn route_statement(statement: &mut StructStatement, router: &RdbRouter) {
        match statement {
            StructStatement::MysqlCreateTable { statement } => {
                let (db, tb) = (
                    statement.table.database_name.clone(),
                    statement.table.table_name.clone(),
                );
                let (db, tb) = router.get_tb_map(&db, &tb);

                statement.table.database_name = db.to_string();
                statement.table.table_name = tb.to_string();

                for index in statement.indexes.iter_mut() {
                    index.database_name = db.to_string();
                    index.table_name = tb.to_string();
                }

                for constraint in statement.constraints.iter_mut() {
                    constraint.database_name = db.to_string();
                    constraint.table_name = tb.to_string();
                }
            }

            StructStatement::MysqlCreateDatabase { statement } => {
                statement.database.name = router.get_db_map(&statement.database.name).to_string();
            }

            StructStatement::PgCreateTable { statement } => {
                let (schema, tb) = (
                    statement.table.schema_name.clone(),
                    statement.table.table_name.clone(),
                );
                let (schema, tb) = router.get_tb_map(&schema, &tb);

                statement.table.schema_name = schema.to_string();
                statement.table.table_name = tb.to_string();

                for comment in statement.table_comments.iter_mut() {
                    comment.schema_name = schema.to_string();
                    comment.table_name = tb.to_string();
                }

                for comment in statement.column_comments.iter_mut() {
                    comment.schema_name = schema.to_string();
                    comment.table_name = tb.to_string();
                }

                for constraint in statement.constraints.iter_mut() {
                    constraint.schema_name = schema.to_string();
                    constraint.table_name = tb.to_string();
                }

                for index in statement.indexes.iter_mut() {
                    index.schema_name = schema.to_string();
                    index.table_name = tb.to_string();
                }

                for sequence in statement.sequences.iter_mut() {
                    sequence.schema_name = schema.to_string();
                }

                for owner in statement.sequence_owners.iter_mut() {
                    owner.schema_name = schema.to_string();
                    owner.table_name = tb.to_string();
                }
            }

            StructStatement::PgCreateSchema { statement } => {
                statement.schema.name = router.get_db_map(&statement.schema.name).to_string();
            }

            _ => {}
        }
    }

    pub async fn sink_structs(
        conn_pool: &DBConnPool,
        conflict_policy: &ConflictPolicyEnum,
        statements: Vec<StructStatement>,
        filter: &RdbFilter,
    ) -> anyhow::Result<()> {
        for mut s in statements {
            for (_, sql) in s.to_sqls(filter).iter() {
                log_info!("ddl begin: {}", sql);
                match Self::execute(conn_pool, sql).await {
                    Ok(()) => {
                        log_info!("ddl succeed");
                    }

                    Err(error) => {
                        log_error!("ddl failed, error: {}", error);
                        match conflict_policy {
                            ConflictPolicyEnum::Interrupt => bail! {error},
                            ConflictPolicyEnum::Ignore => {}
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn execute(pool: &DBConnPool, sql: &str) -> anyhow::Result<()> {
        match pool {
            DBConnPool::MySQL(pool) => match query(sql).execute(pool).await {
                Ok(_) => Ok(()),
                Err(error) => bail! {Error::SqlxError(error)},
            },
            DBConnPool::PostgreSQL(pool) => match query(sql).execute(pool).await {
                Ok(_) => Ok(()),
                Err(error) => bail! {Error::SqlxError(error)},
            },
        }
    }
}
