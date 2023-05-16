use crate::{
    call_batch_fn, close_conn_pool,
    sinker::{base_sinker::BaseSinker, rdb_router::RdbRouter},
    sql_util::SqlUtil,
    Sinker,
};

use dt_common::{error::Error, log_error};

use dt_meta::{
    ddl_data::DdlData,
    mysql::{mysql_meta_manager::MysqlMetaManager, mysql_tb_meta::MysqlTbMeta},
    row_data::RowData,
    row_type::RowType,
};

use sqlx::{MySql, Pool};

use async_trait::async_trait;

#[derive(Clone)]
pub struct MysqlSinker {
    pub conn_pool: Pool<MySql>,
    pub meta_manager: MysqlMetaManager,
    pub router: RdbRouter,
    pub batch_size: usize,
}

#[async_trait]
impl Sinker for MysqlSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> Result<(), Error> {
        if data.len() == 0 {
            return Ok(());
        }

        if !batch {
            self.serial_sink(data).await.unwrap();
        } else {
            match data[0].row_type {
                RowType::Insert => {
                    call_batch_fn!(self, data, Self::batch_insert);
                }
                RowType::Delete => {
                    call_batch_fn!(self, data, Self::batch_delete);
                }
                _ => self.serial_sink(data).await.unwrap(),
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        return close_conn_pool!(self);
    }

    async fn sink_ddl(&mut self, _data: Vec<DdlData>, _batch: bool) -> Result<(), Error> {
        Ok(())
    }
}

impl MysqlSinker {
    async fn serial_sink(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        for row_data in data.iter() {
            let tb_meta = self.get_tb_meta(&row_data).await?;
            let sql_util = SqlUtil::new_for_mysql(&tb_meta);

            let (mut sql, cols, binds) = sql_util.get_query_info(&row_data)?;
            sql = self.handle_dialect(&sql);
            let query = SqlUtil::create_mysql_query(&sql, &cols, &binds, &tb_meta);
            query.execute(&self.conn_pool).await.unwrap();
        }
        Ok(())
    }

    async fn batch_delete(
        &mut self,
        data: &mut Vec<RowData>,
        start_index: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let tb_meta = self.get_tb_meta(&data[0]).await?;
        let sql_util = SqlUtil::new_for_mysql(&tb_meta);

        let (sql, cols, binds) = sql_util.get_batch_delete_query(&data, start_index, batch_size)?;
        let query = SqlUtil::create_mysql_query(&sql, &cols, &binds, &tb_meta);

        query.execute(&self.conn_pool).await.unwrap();
        Ok(())
    }

    async fn batch_insert(
        &mut self,
        data: &mut Vec<RowData>,
        start_index: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let tb_meta = self.get_tb_meta(&data[0]).await?;
        let sql_util = SqlUtil::new_for_mysql(&tb_meta);

        let (mut sql, cols, binds) =
            sql_util.get_batch_insert_query(&data, start_index, batch_size)?;
        sql = self.handle_dialect(&sql);
        let query = SqlUtil::create_mysql_query(&sql, &cols, &binds, &tb_meta);

        let result = query.execute(&self.conn_pool).await;
        if let Err(error) = result {
            log_error!(
                "batch insert failed, will insert one by one, schema: {}, tb: {}, error: {}",
                tb_meta.basic.schema,
                tb_meta.basic.tb,
                error.to_string()
            );
            // insert one by one
            let sub_data = &data[start_index..start_index + batch_size];
            self.serial_sink(sub_data.to_vec()).await.unwrap();
        }
        Ok(())
    }

    #[inline(always)]
    fn handle_dialect(&self, sql: &str) -> String {
        sql.replace("INSERT", "REPLACE")
    }

    #[inline(always)]
    async fn get_tb_meta(&mut self, row_data: &RowData) -> Result<MysqlTbMeta, Error> {
        BaseSinker::get_mysql_tb_meta(&mut self.meta_manager, &mut self.router, row_data).await
    }
}
