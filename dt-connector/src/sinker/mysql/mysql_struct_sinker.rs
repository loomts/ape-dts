use crate::{
    close_conn_pool,
    sinker::base_struct_sinker::{BaseStructSinker, DBConnPool},
    Sinker,
};

use dt_common::{
    config::config_enums::ConflictPolicyEnum, error::Error, utils::rdb_filter::RdbFilter,
};

use dt_common::meta::ddl_data::DdlData;

use sqlx::{MySql, Pool};

use async_trait::async_trait;

#[derive(Clone)]
pub struct MysqlStructSinker {
    pub conn_pool: Pool<MySql>,
    pub conflict_policy: ConflictPolicyEnum,
    pub filter: RdbFilter,
}

#[async_trait]
impl Sinker for MysqlStructSinker {
    async fn sink_ddl(&mut self, data: Vec<DdlData>, _batch: bool) -> Result<(), Error> {
        BaseStructSinker::sink_ddl(
            &DBConnPool::MySQL(self.conn_pool.clone()),
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
