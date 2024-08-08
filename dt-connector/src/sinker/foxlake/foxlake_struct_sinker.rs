use crate::{
    close_conn_pool,
    rdb_router::RdbRouter,
    sinker::base_struct_sinker::{BaseStructSinker, DBConnPool},
    Sinker,
};

use dt_common::{
    config::config_enums::ConflictPolicyEnum,
    meta::struct_meta::{statement::struct_statement::StructStatement, struct_data::StructData},
    rdb_filter::RdbFilter,
};

use sqlx::{MySql, Pool};

use async_trait::async_trait;

#[derive(Clone)]
pub struct FoxlakeStructSinker {
    pub conn_pool: Pool<MySql>,
    pub conflict_policy: ConflictPolicyEnum,
    pub filter: RdbFilter,
    pub router: RdbRouter,
    pub engine: String,
}

#[async_trait]
impl Sinker for FoxlakeStructSinker {
    async fn sink_struct(&mut self, data: Vec<StructData>) -> anyhow::Result<()> {
        let mut statements = Vec::new();
        for mut struct_data in data {
            BaseStructSinker::route_statement(&mut struct_data.statement, &self.router);

            match &mut struct_data.statement {
                StructStatement::MysqlCreateTable { statement } => {
                    statement.table.table_collation = String::new();
                    statement.table.engine_name = format!("'{}'", self.engine);
                    for column in statement.table.columns.iter_mut() {
                        column.collation_name = String::new();
                    }
                }

                StructStatement::MysqlCreateDatabase { statement } => {
                    statement.database.default_collation_name = String::new();
                }

                _ => {}
            }

            statements.push(struct_data.statement);
        }

        BaseStructSinker::sink_structs(
            &DBConnPool::MySQL(self.conn_pool.clone()),
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
