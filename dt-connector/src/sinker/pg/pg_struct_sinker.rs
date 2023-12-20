use crate::{
    close_conn_pool,
    sinker::base_struct_sinker::{BaseStructSinker, DBConnPool},
    Sinker,
};

use dt_common::{
    config::config_enums::ConflictPolicyEnum, error::Error, utils::rdb_filter::RdbFilter,
};

use dt_meta::ddl_data::DdlData;

use async_trait::async_trait;
use sqlx::{Pool, Postgres};

#[derive(Clone)]
pub struct PgStructSinker {
    pub conn_pool: Pool<Postgres>,
    pub conflict_policy: ConflictPolicyEnum,
    pub filter: RdbFilter,
}

#[async_trait]
impl Sinker for PgStructSinker {
    async fn sink_ddl(&mut self, data: Vec<DdlData>, _batch: bool) -> Result<(), Error> {
        BaseStructSinker::sink_ddl(
            &DBConnPool::PostgreSQL(self.conn_pool.clone()),
            &self.conflict_policy,
            data,
            &self.filter,
        )
        .await
    }

    async fn close(&mut self) -> Result<(), Error> {
        return close_conn_pool!(self);
    }
}

impl PgStructSinker {}
