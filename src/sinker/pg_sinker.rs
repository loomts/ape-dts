use sqlx::{Pool, Postgres};

use crate::{
    error::Error,
    meta::{
        pg::{pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
        row_data::RowData,
    },
    traits::{sqlx_ext::SqlxExt, traits::Sinker},
};

use super::{rdb_router::RdbRouter, rdb_util::RdbUtil};

use async_trait::async_trait;

pub struct PgSinker {
    pub conn_pool: Pool<Postgres>,
    pub meta_manager: PgMetaManager,
    pub router: RdbRouter,
    pub batch_size: usize,
}

#[async_trait]
impl Sinker for PgSinker {
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

impl PgSinker {
    async fn sink_internal(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        for row_data in data.iter() {
            let tb_meta = self.get_tb_meta(&row_data).await?;
            let rdb_util = RdbUtil::new_for_pg(tb_meta);

            let (mut sql, binds) = rdb_util.get_query(&row_data)?;
            sql = self.replace_placeholder(&sql);
            let mut query = sqlx::query(&sql);
            for bind in binds {
                query = query.bind_col_value(bind);
            }

            let result = query.execute(&self.conn_pool).await.unwrap();
            rdb_util.check_result(result.rows_affected(), 1, &sql, row_data)?;
        }
        Ok(())
    }

    async fn batch_insert(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        let all_count = data.len();
        let mut sinked_count = 0;

        let first_row_data = &data[0];
        let tb_meta = self.get_tb_meta(first_row_data).await?;
        let rdb_util = RdbUtil::new_for_pg(tb_meta);

        loop {
            let mut batch_size = self.batch_size;
            if all_count - sinked_count < batch_size {
                batch_size = all_count - sinked_count;
            }

            let (mut sql, binds) =
                rdb_util.get_batch_insert_query(&data, sinked_count, batch_size)?;
            sql = self.replace_placeholder(&sql);
            let mut query = sqlx::query(&sql);
            for bind in binds {
                query = query.bind_col_value(bind);
            }

            let result = query.execute(&self.conn_pool).await.unwrap();
            rdb_util.check_result(
                result.rows_affected(),
                batch_size as u64,
                &sql,
                first_row_data,
            )?;

            sinked_count += batch_size;
            if sinked_count == all_count {
                break;
            }
        }

        Ok(())
    }

    async fn get_tb_meta(&mut self, row_data: &RowData) -> Result<PgTbMeta, Error> {
        let (db, tb) = self.router.get_route(&row_data.db, &row_data.tb);
        let tb_meta = self.meta_manager.get_tb_meta(&db, &tb).await?;
        return Ok(tb_meta);
    }

    fn replace_placeholder(&self, sql: &str) -> String {
        let mut new_sql = sql.to_string();
        let mut i = 1;

        // TODO, handle ON CONFLICT cases
        new_sql = new_sql.replace("REPLACE", "INSERT");
        while new_sql.contains("?") {
            let placeholder = format!("${}", i.to_string());
            new_sql = new_sql.replacen("?", &placeholder, 1);
            i += 1;
        }
        new_sql
    }
}
