use crate::{
    call_batch_fn, close_conn_pool,
    rdb_query_builder::RdbQueryBuilder,
    sinker::{base_sinker::BaseSinker, rdb_router::RdbRouter},
    Sinker,
};

use dt_common::{config::config_enums::DbType, error::Error, log_error, utils::sql_util::SqlUtil};
use sqlx::{Pool, Postgres};

use dt_meta::{
    col_value::ColValue,
    ddl_data::DdlData,
    pg::{pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
    row_data::RowData,
    row_type::RowType,
};

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

impl PgSinker {
    async fn serial_sink(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        for row_data in data.iter() {
            let tb_meta = self.get_tb_meta(&row_data).await?;
            let query_builder = RdbQueryBuilder::new_for_pg(&tb_meta);

            let (sql, cols, binds) = if row_data.row_type == RowType::Insert {
                self.get_insert_query(&query_builder, &tb_meta, row_data)?
            } else {
                query_builder.get_query_info(&row_data)?
            };
            let query = query_builder.create_pg_query(&sql, &cols, &binds);
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
        let query_builder = RdbQueryBuilder::new_for_pg(&tb_meta);

        let (sql, cols, binds) =
            query_builder.get_batch_delete_query(&data, start_index, batch_size)?;
        let query = query_builder.create_pg_query(&sql, &cols, &binds);

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
        let query_builder = RdbQueryBuilder::new_for_pg(&tb_meta);

        let (sql, cols, binds) =
            query_builder.get_batch_insert_query(&data, start_index, batch_size)?;
        let query = query_builder.create_pg_query(&sql, &cols, &binds);

        let result = query.execute(&self.conn_pool).await;
        if let Err(error) = result {
            log_error!(
                "batch insert failed, will insert one by one, schema: {}, tb: {}, error: {}",
                tb_meta.basic.schema,
                tb_meta.basic.tb,
                error.to_string()
            );
            let sub_data = &data[start_index..start_index + batch_size];
            self.serial_sink(sub_data.to_vec()).await.unwrap();
        }
        Ok(())
    }

    fn get_insert_query<'a>(
        &self,
        query_builder: &RdbQueryBuilder,
        tb_meta: &PgTbMeta,
        row_data: &'a RowData,
    ) -> Result<(String, Vec<String>, Vec<Option<&'a ColValue>>), Error> {
        let (mut sql, mut cols, mut binds) = query_builder.get_insert_query(row_data)?;

        let mut placeholder_index = cols.len() + 1;
        let after = row_data.after.as_ref().unwrap();
        let mut set_pairs = Vec::new();
        for col in tb_meta.basic.cols.iter() {
            let set_pair = format!(
                r#""{}"={}"#,
                col,
                query_builder.get_placeholder(placeholder_index, col)
            );
            set_pairs.push(set_pair);
            cols.push(col.clone());
            binds.push(after.get(col));
            placeholder_index += 1;
        }

        sql = format!(
            "{} ON CONFLICT ({}) DO UPDATE SET {}",
            sql,
            SqlUtil::escape_cols(&tb_meta.basic.id_cols, &DbType::Pg).join(","),
            set_pairs.join(",")
        );
        Ok((sql, cols, binds))
    }

    #[inline(always)]
    async fn get_tb_meta(&mut self, row_data: &RowData) -> Result<PgTbMeta, Error> {
        BaseSinker::get_pg_tb_meta(&mut self.meta_manager, &mut self.router, row_data).await
    }
}
