use crate::{
    error::Error,
    meta::{
        col_value::ColValue,
        pg::{pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
        row_data::RowData,
        row_type::RowType,
    },
    sqlx_ext::SqlxPg,
    traits::Sinker,
};
use log::error;
use sqlx::{Pool, Postgres};

use super::{rdb_router::RdbRouter, rdb_sinker_util::RdbSinkerUtil};

use async_trait::async_trait;

#[derive(Clone)]
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

            let (mut sql, cols, binds) = if row_data.row_type == RowType::Insert {
                self.get_insert_query(&rdb_util, &tb_meta, row_data)?
            } else {
                rdb_util.get_query(&row_data)?
            };
            sql = self.handle_dialect(&tb_meta, &sql, &cols);
            let mut query = sqlx::query(&sql);

            let mut i = 0;
            for bind in binds {
                let col_type = tb_meta.col_type_map.get(&cols[i]).unwrap();
                query = query.bind_col_value(bind, col_type);
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

        loop {
            let mut batch_size = self.batch_size;
            if all_count - sinked_count < batch_size {
                batch_size = all_count - sinked_count;
            }

            let (sql, cols, binds) =
                self.get_batch_insert_query(&tb_meta, &data, sinked_count, batch_size)?;
            let mut query = sqlx::query(&sql);
            for i in 0..binds.len() {
                let col_type = tb_meta.col_type_map.get(&cols[i]).unwrap();
                query = query.bind_col_value(binds[i], col_type);
            }

            let result = query.execute(&self.conn_pool).await;
            if let Err(error) = result {
                error!(
                    "batch insert failed, will insert one by one, schema: {}, tb: {}, error: {}",
                    tb_meta.schema,
                    tb_meta.tb,
                    error.to_string()
                );
                // insert one by one
                let slice_data = &data[sinked_count..sinked_count + batch_size];
                self.sink_internal(slice_data.to_vec()).await.unwrap();
            }

            sinked_count += batch_size;
            if sinked_count == all_count {
                break;
            }
        }

        Ok(())
    }

    #[inline(always)]
    async fn get_tb_meta(&mut self, row_data: &RowData) -> Result<PgTbMeta, Error> {
        let (db, tb) = self.router.get_route(&row_data.db, &row_data.tb);
        let tb_meta = self.meta_manager.get_tb_meta(&db, &tb).await?;
        return Ok(tb_meta);
    }

    fn get_insert_query<'a>(
        &self,
        rdb_util: &RdbSinkerUtil,
        tb_meta: &PgTbMeta,
        row_data: &'a RowData,
    ) -> Result<(String, Vec<String>, Vec<Option<&'a ColValue>>), Error> {
        let (mut sql, mut cols, mut binds) = rdb_util.get_insert_query(row_data)?;

        let after = row_data.after.as_ref().unwrap();
        let mut set_pairs = Vec::new();
        for col in &tb_meta.cols {
            if tb_meta.where_cols.contains(col) {
                continue;
            }
            set_pairs.push(format!("{}=?", col));
            cols.push(col.clone());
            binds.push(after.get(col));
        }

        sql = format!(
            "{} ON CONFLICT ({}) DO UPDATE SET {}",
            sql,
            tb_meta.where_cols.join(","),
            set_pairs.join(",")
        );
        Ok((sql, cols, binds))
    }

    #[inline(always)]
    fn handle_dialect(&self, tb_meta: &PgTbMeta, sql: &str, cols: &Vec<String>) -> String {
        // INSERT INTO tb_1(col_1, col_2) VALUES ('{"bar": "baz"}'::json, 'a'), ('{"bar": "baz"}'::json, 'b');
        let mut i = 1;
        let mut new_sql = sql.to_string();
        while new_sql.contains("?") {
            let col_index = (i - 1) % cols.len();
            let col_type = tb_meta.col_type_map.get(&cols[col_index]).unwrap();

            // workaround for types like bit(3)
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

    pub fn get_batch_insert_query<'a>(
        &self,
        tb_meta: &PgTbMeta,
        data: &'a Vec<RowData>,
        start_index: usize,
        batch_size: usize,
    ) -> Result<(String, Vec<String>, Vec<Option<&'a ColValue>>), Error> {
        let mut i = 1;
        let mut row_values = Vec::new();
        for _ in 0..batch_size {
            let mut col_values = Vec::new();
            for col in tb_meta.cols.iter() {
                let col_type = tb_meta.col_type_map.get(col).unwrap();

                // workaround for types like bit(3)
                let col_type_name = if col_type.short_name == "bit" {
                    "varbit"
                } else {
                    &col_type.short_name
                };

                let placeholder = format!("${}::{}", i, col_type_name);
                col_values.push(placeholder);
                i += 1;
            }
            let col_values_str = format!("({})", col_values.join(","));
            row_values.push(col_values_str);
        }

        let sql = format!(
            "INSERT INTO {}.{}({}) VALUES{}",
            tb_meta.schema,
            tb_meta.tb,
            tb_meta.cols.join(","),
            row_values.join(",")
        );

        let mut cols = Vec::new();
        let mut binds = Vec::new();
        for i in start_index..start_index + batch_size {
            let row_data = &data[i];
            let after = row_data.after.as_ref().unwrap();
            for col_name in tb_meta.cols.iter() {
                cols.push(col_name.clone());
                binds.push(after.get(col_name));
            }
        }

        Ok((sql, cols, binds))
    }
}
