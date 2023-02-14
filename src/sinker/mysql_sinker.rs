use log::info;
use sqlx::{mysql::MySqlQueryResult, MySql, Pool};

use crate::{
    error::Error,
    ext::sqlx_ext::SqlxExt,
    meta::{db_meta_manager::DbMetaManager, row_data::RowData, row_type::RowType, tb_meta::TbMeta},
};

use super::{router::Router, sql_util::SqlUtil, traits::Sinker};

use async_trait::async_trait;

pub struct MysqlSinker {
    pub conn_pool: Pool<MySql>,
    pub db_meta_manager: DbMetaManager,
    pub router: Router,
    pub batch_size: usize,
}

#[async_trait]
impl Sinker for MysqlSinker {
    async fn sink(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        if data.len() == 0 {
            return Ok(());
        }

        // currently only support batch insert
        if self.batch_size > 1 {
            self.batch_insert(data).await
        } else {
            self.sink_internal(data).await
        }
    }
}

impl MysqlSinker {
    async fn sink_internal(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        for row_data in data.iter() {
            let tb_meta = self.get_tb_meta(&row_data).await?;
            let (sql, binds) = match row_data.row_type {
                RowType::Insert => SqlUtil::get_insert_sql(row_data, &tb_meta)?,
                RowType::Update => SqlUtil::get_update_sql(row_data, &tb_meta)?,
                RowType::Delete => SqlUtil::get_delete_sql(row_data, &tb_meta)?,
            };

            let mut query = sqlx::query(&sql);
            for bind in binds {
                query = query.bind_col_value(bind);
            }

            let result = query.execute(&self.conn_pool).await.unwrap();
            self.check_result(result, 1, &sql, row_data, &tb_meta)
                .await?;
        }
        Ok(())
    }

    async fn batch_insert(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        let all_count = data.len();
        let mut sinked_count = 0;

        let first_row_data = &data[0];
        let tb_meta = self.get_tb_meta(first_row_data).await?;

        loop {
            let batch_size = if all_count > sinked_count + self.batch_size {
                self.batch_size
            } else {
                all_count - sinked_count
            };

            let sql = SqlUtil::get_batch_insert_sql(&tb_meta, batch_size)?;
            let mut query = sqlx::query(&sql);

            for i in sinked_count..sinked_count + batch_size {
                let row_data = &data[i];
                let after = row_data.after.as_ref().unwrap();
                for col_name in tb_meta.cols.iter() {
                    query = query.bind_col_value(after.get(col_name));
                }
            }

            let result = query.execute(&self.conn_pool).await.unwrap();
            self.check_result(result, batch_size as u64, &sql, first_row_data, &tb_meta)
                .await?;

            sinked_count += batch_size;
            if sinked_count == all_count {
                break;
            }
        }

        Ok(())
    }

    async fn get_tb_meta(&mut self, row_data: &RowData) -> Result<TbMeta, Error> {
        let (db, tb) = self.router.get_route(&row_data.db, &row_data.tb);
        let tb_meta = self.db_meta_manager.get_tb_meta(&db, &tb).await?;
        return Ok(tb_meta);
    }

    async fn check_result(
        &self,
        result: MySqlQueryResult,
        rows_affected_expected: u64,
        sql: &str,
        row_data: &RowData,
        tb_meta: &TbMeta,
    ) -> Result<(), Error> {
        if result.rows_affected() != rows_affected_expected {
            info!(
                "sql: {}\nrows_affected: {},rows_affected_expected: {}\n{}",
                sql,
                result.rows_affected(),
                rows_affected_expected,
                row_data.to_string(tb_meta)
            );
        }
        Ok(())
    }
}
