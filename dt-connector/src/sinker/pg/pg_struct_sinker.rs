use crate::{
    close_conn_pool,
    rdb_router::RdbRouter,
    sinker::base_struct_sinker::{BaseStructSinker, DBConnPool},
    Sinker,
};

use dt_common::{
    config::config_enums::ConflictPolicyEnum, meta::struct_meta::struct_data::StructData,
    rdb_filter::RdbFilter,
};

use async_trait::async_trait;
use sqlx::{Pool, Postgres};

#[derive(Clone)]
pub struct PgStructSinker {
    pub conn_pool: Pool<Postgres>,
    pub conflict_policy: ConflictPolicyEnum,
    pub filter: RdbFilter,
    pub router: RdbRouter,
}

#[async_trait]
impl Sinker for PgStructSinker {
    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        let mut statements = Vec::new();
        for mut struct_data in data {
            BaseStructSinker::route_statement(&mut struct_data.statement, &self.router);
            statements.push(struct_data.statement);
        }

        BaseStructSinker::sink_structs(
            &DBConnPool::PostgreSQL(self.conn_pool.clone()),
            &self.conflict_policy,
            statements,
            &self.filter,
        )
        .await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        return close_conn_pool!(self);
    }
}

impl PgStructSinker {}
