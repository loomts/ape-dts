use crate::{
    common::sql_util::SqlUtil,
    error,
    error::Error,
    meta::{
        mysql::{mysql_meta_manager::MysqlMetaManager, mysql_tb_meta::MysqlTbMeta},
        row_data::RowData,
        row_type::RowType,
    },
    sinker::{rdb_router::RdbRouter, sinker_util::SinkerUtil},
    traits::Sinker,
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
    async fn sink(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        if data.len() == 0 {
            return Ok(());
        }
        self.serial_sink(data).await
    }

    async fn close(&mut self) -> Result<(), Error> {
        if self.conn_pool.is_closed() {
            return Ok(());
        }
        return Ok(self.conn_pool.close().await);
    }

    async fn batch_sink(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        if data.len() == 0 {
            return Ok(());
        }

        match &data[0].row_type {
            RowType::Insert => self.batch_insert(data).await,
            RowType::Delete => self.batch_delete(data).await,
            _ => self.serial_sink(data).await,
        }
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

    async fn batch_delete(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        let all_count = data.len();
        let mut sinked_count = 0;
        let tb_meta = self.get_tb_meta(&data[0]).await?;
        let sql_util = SqlUtil::new_for_mysql(&tb_meta);

        loop {
            let mut batch_size = self.batch_size;
            if all_count - sinked_count < batch_size {
                batch_size = all_count - sinked_count;
            }

            let (sql, cols, binds) =
                sql_util.get_batch_delete_query(&data, sinked_count, batch_size)?;
            let query = SqlUtil::create_mysql_query(&sql, &cols, &binds, &tb_meta);
            query.execute(&self.conn_pool).await.unwrap();

            sinked_count += batch_size;
            if sinked_count == all_count {
                break;
            }
        }
        Ok(())
    }

    async fn batch_insert(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        let all_count = data.len();
        let mut sinked_count = 0;
        let tb_meta = self.get_tb_meta(&data[0]).await?;
        let sql_util = SqlUtil::new_for_mysql(&tb_meta);

        loop {
            let mut batch_size = self.batch_size;
            if all_count - sinked_count < batch_size {
                batch_size = all_count - sinked_count;
            }

            let (mut sql, cols, binds) =
                sql_util.get_batch_insert_query(&data, sinked_count, batch_size)?;
            sql = self.handle_dialect(&sql);
            let query = SqlUtil::create_mysql_query(&sql, &cols, &binds, &tb_meta);

            let result = query.execute(&self.conn_pool).await;
            if let Err(error) = result {
                error!(
                    "batch insert failed, will insert one by one, schema: {}, tb: {}, error: {}",
                    tb_meta.basic.schema,
                    tb_meta.basic.tb,
                    error.to_string()
                );
                // insert one by one
                let sub_data = &data[sinked_count..sinked_count + batch_size];
                self.serial_sink(sub_data.to_vec()).await.unwrap();
            }

            sinked_count += batch_size;
            if sinked_count == all_count {
                break;
            }
        }

        Ok(())
    }

    #[inline(always)]
    fn handle_dialect(&self, sql: &str) -> String {
        sql.replace("INSERT", "REPLACE")
    }

    #[inline(always)]
    async fn get_tb_meta(&mut self, row_data: &RowData) -> Result<MysqlTbMeta, Error> {
        SinkerUtil::get_mysql_tb_meta(&mut self.meta_manager, &mut self.router, row_data).await
    }
}
