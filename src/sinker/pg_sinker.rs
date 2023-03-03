use sqlx::{Pool, Postgres};

use crate::{
    error::Error,
    meta::{
        pg::{pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
        row_data::RowData,
    },
    traits::{sqlx_ext::SqlxPg, traits::Sinker},
};

use super::{rdb_router::RdbRouter, rdb_sinker_util::RdbSinkerUtil};

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

    async fn close(&mut self) -> Result<(), Error> {
        if self.conn_pool.is_closed() {
            return Ok(());
        }
        return Ok(self.conn_pool.close().await);
    }
}

impl PgSinker {
    async fn sink_internal(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        for row_data in data.iter() {
            let tb_meta = self.get_tb_meta(&row_data).await?;
            let rdb_util = RdbSinkerUtil::new_for_pg(tb_meta.clone());

            let (mut sql, cols, binds) = rdb_util.get_query(&row_data)?;
            sql = self.replace_placeholder(&tb_meta, &sql, &cols);
            let mut query = sqlx::query(&sql);

            let mut i = 0;
            for bind in binds {
                let col_type = tb_meta.col_type_map.get(&cols[i]).unwrap();
                println!("-----------: {}", cols[i]);
                query = query.bind_col_value(bind);
                i += 1;
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
        let rdb_util = RdbSinkerUtil::new_for_pg(tb_meta.clone());

        loop {
            let mut batch_size = self.batch_size;
            if all_count - sinked_count < batch_size {
                batch_size = all_count - sinked_count;
            }

            let (mut sql, cols, binds) =
                rdb_util.get_batch_insert_query(&data, sinked_count, batch_size)?;
            sql = self.replace_placeholder(&tb_meta, &sql, &cols);
            let mut query = sqlx::query(&sql);

            let mut i = 0;
            for bind in binds {
                let col_type = tb_meta.col_type_map.get(&cols[i]).unwrap();
                println!("-----------: {}", cols[i]);
                query = query.bind_col_value(bind);
                i += 1;
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

    fn replace_placeholder(&self, tb_meta: &PgTbMeta, sql: &str, cols: &Vec<String>) -> String {
        let mut new_sql = sql.to_string();
        let mut i = 1;

        // INSERT INTO tb_1(col_1, col_2) VALUES ('{"bar": "baz"}'::json, 'a'), ('{"bar": "baz"}'::json, 'b');

        // TODO, handle ON CONFLICT cases
        new_sql = new_sql.replace("REPLACE", "INSERT");
        while new_sql.contains("?") {
            let col_index = (i - 1) % cols.len();
            let col_type = tb_meta.col_type_map.get(&cols[col_index]).unwrap();
            // TODO, workaround for bit
            let col_type_name = if col_type.short_name == "bit" {
                "varbit"
            } else {
                &col_type.short_name
            };

            let placeholder = format!("${}::{}", i.to_string(), col_type_name);
            new_sql = new_sql.replacen("?", &placeholder, 1);
            i += 1;
        }
        new_sql
    }
}
