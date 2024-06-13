use crate::{
    close_conn_pool,
    rdb_router::RdbRouter,
    sinker::base_struct_sinker::{BaseStructSinker, DBConnPool},
    Sinker,
};

use dt_common::{
    config::config_enums::ConflictPolicyEnum,
    meta::struct_meta::statement::struct_statement::StructStatement, rdb_filter::RdbFilter,
};

use dt_common::meta::ddl_data::DdlData;
use sqlx::{MySql, Pool};

use async_trait::async_trait;

#[derive(Clone)]
pub struct FoxlakeStructSinker {
    pub conn_pool: Pool<MySql>,
    pub conflict_policy: ConflictPolicyEnum,
    pub filter: RdbFilter,
    pub router: RdbRouter,
}

#[async_trait]
impl Sinker for FoxlakeStructSinker {
    async fn sink_ddl(&mut self, mut data: Vec<DdlData>, _batch: bool) -> anyhow::Result<()> {
        for ddl_data in data.iter_mut() {
            match ddl_data.statement.as_mut() {
                Some(StructStatement::MysqlCreateTable { statement }) => {
                    let db = self.router.get_db_map(&statement.table.database_name);
                    // currently, foxlake does not support index
                    for index in statement.indexes.iter_mut() {
                        index.database_name = db.to_string();
                    }
                    for constraint in statement.constraints.iter_mut() {
                        constraint.database_name = db.to_string();
                    }
                    statement.table.database_name = db.to_string();

                    statement.table.table_collation = String::new();
                    statement.table.engine_name = String::new();
                    for column in statement.table.columns.iter_mut() {
                        column.collation_name = String::new();
                    }
                }

                Some(StructStatement::MysqlCreateDatabase { statement }) => {
                    statement.database.name =
                        self.router.get_db_map(&statement.database.name).to_string();

                    statement.database.default_collation_name = String::new();
                }

                _ => {}
            }
        }

        BaseStructSinker::sink_ddl(
            &DBConnPool::MySQL(self.conn_pool.clone()),
            &self.conflict_policy,
            data,
            &self.filter,
        )
        .await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        return close_conn_pool!(self);
    }
}
