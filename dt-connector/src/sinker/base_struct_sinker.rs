use anyhow::bail;
use dt_common::log_error;
use dt_common::meta::struct_meta::struct_data::StructData;
use dt_common::{
    config::config_enums::ConflictPolicyEnum, error::Error, log_info, rdb_filter::RdbFilter,
};
use sqlx::{query, MySql, Pool, Postgres};

pub struct BaseStructSinker {}

pub enum DBConnPool {
    MySQL(Pool<MySql>),
    PostgreSQL(Pool<Postgres>),
}

impl BaseStructSinker {
    pub async fn sink_structs(
        conn_pool: &DBConnPool,
        conflict_policy: &ConflictPolicyEnum,
        data: Vec<StructData>,
        filter: &RdbFilter,
    ) -> anyhow::Result<()> {
        for mut struct_data in data {
            for (_, sql) in struct_data.statement.to_sqls(filter)?.iter() {
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
