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
            let rdb_util = RdbSinkerUtil::new_for_pg(&tb_meta);

            let (sql, cols, binds) = if row_data.row_type == RowType::Insert {
                self.get_insert_query(&rdb_util, &tb_meta, row_data)?
            } else {
                rdb_util.get_query(&row_data)?
            };

            let mut query = sqlx::query(&sql);
            for i in 0..binds.len() {
                let col_type = tb_meta.col_type_map.get(&cols[i]).unwrap();
                query = query.bind_col_value(binds[i], col_type);
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
        let rdb_util = RdbSinkerUtil::new_for_pg(&tb_meta);

        loop {
            let mut batch_size = self.batch_size;
            if all_count - sinked_count < batch_size {
                batch_size = all_count - sinked_count;
            }

            let (sql, cols, binds) =
                rdb_util.get_batch_insert_query(&data, sinked_count, batch_size)?;
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

    fn get_insert_query<'a>(
        &self,
        rdb_util: &RdbSinkerUtil,
        tb_meta: &PgTbMeta,
        row_data: &'a RowData,
    ) -> Result<(String, Vec<String>, Vec<Option<&'a ColValue>>), Error> {
        let (mut sql, mut cols, mut binds) = rdb_util.get_insert_query(row_data)?;

        let mut placeholder_index = cols.len() + 1;
        let after = row_data.after.as_ref().unwrap();
        let mut set_pairs = Vec::new();
        for col in &tb_meta.cols {
            if tb_meta.where_cols.contains(col) {
                continue;
            }
            let set_pair = format!(
                "{}={}",
                col,
                rdb_util.get_placeholder(placeholder_index, col)
            );
            set_pairs.push(set_pair);
            cols.push(col.clone());
            binds.push(after.get(col));
            placeholder_index += 1;
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
    async fn get_tb_meta(&mut self, row_data: &RowData) -> Result<PgTbMeta, Error> {
        let (db, tb) = self.router.get_route(&row_data.db, &row_data.tb);
        let tb_meta = self.meta_manager.get_tb_meta(&db, &tb).await?;
        return Ok(tb_meta);
    }
}
