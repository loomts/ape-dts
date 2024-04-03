use dt_common::meta::ddl_data::DdlData;
use dt_common::{
    config::config_enums::ConflictPolicyEnum, error::Error, log_info, utils::rdb_filter::RdbFilter,
};
use sqlx::{query, MySql, Pool, Postgres};

pub struct BaseStructSinker {}

pub enum DBConnPool {
    MySQL(Pool<MySql>),
    PostgreSQL(Pool<Postgres>),
}

impl BaseStructSinker {
    pub async fn sink_ddl(
        conn_pool: &DBConnPool,
        conflict_policy: &ConflictPolicyEnum,
        data: Vec<DdlData>,
        filter: &RdbFilter,
    ) -> Result<(), Error> {
        for ddl_data in data {
            let mut statement = ddl_data.statement.unwrap();
            for (_, sql) in statement.to_sqls(filter).iter() {
                log_info!("ddl begin: {}", sql);
                match Self::execute(conn_pool, sql).await {
                    Ok(()) => {
                        log_info!("ddl succeed");
                    }

                    Err(error) => {
                        log_info!("ddl failed, error: {}", error);
                        match conflict_policy {
                            ConflictPolicyEnum::Interrupt => return Err(error),
                            ConflictPolicyEnum::Ignore => {}
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn execute(pool: &DBConnPool, sql: &str) -> Result<(), Error> {
        match pool {
            DBConnPool::MySQL(pool) => match query(sql).execute(pool).await {
                Ok(_) => Ok(()),
                Err(error) => Err(Error::SqlxError(error)),
            },
            DBConnPool::PostgreSQL(pool) => match query(sql).execute(pool).await {
                Ok(_) => Ok(()),
                Err(error) => Err(Error::SqlxError(error)),
            },
        }
    }
}
