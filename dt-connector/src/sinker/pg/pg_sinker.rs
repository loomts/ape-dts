use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use crate::{
    call_batch_fn, close_conn_pool,
    rdb_query_builder::{RdbQueryBuilder, RdbQueryInfo},
    rdb_router::RdbRouter,
    sinker::base_sinker::BaseSinker,
    Sinker,
};

use dt_common::{
    config::config_enums::DbType, error::Error, log_error, monitor::monitor::Monitor,
    utils::sql_util::SqlUtil,
};
use sqlx::{Pool, Postgres};

use dt_meta::{
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
    pub monitor: Arc<Mutex<Monitor>>,
}

#[async_trait]
impl Sinker for PgSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> Result<(), Error> {
        if data.is_empty() {
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
}

impl PgSinker {
    async fn serial_sink(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        let start_time = Instant::now();
        let mut data_size = 0;

        for row_data in data.iter() {
            data_size += row_data.data_size;

            let tb_meta = self.meta_manager.get_tb_meta_by_row_data(&row_data).await?;
            let query_builder = RdbQueryBuilder::new_for_pg(tb_meta);

            let query_info = if row_data.row_type == RowType::Insert {
                Self::get_insert_query(&query_builder, tb_meta, row_data)?
            } else {
                query_builder.get_query_info(row_data)?
            };
            let query = query_builder.create_pg_query(&query_info);
            query.execute(&self.conn_pool).await.unwrap();
        }

        BaseSinker::update_serial_monitor(&mut self.monitor, data.len(), data_size, start_time)
            .await
    }

    async fn batch_delete(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let start_time = Instant::now();

        let tb_meta = self.meta_manager.get_tb_meta_by_row_data(&data[0]).await?;
        let query_builder = RdbQueryBuilder::new_for_pg(tb_meta);

        let (query_info, data_size) =
            query_builder.get_batch_delete_query(data, start_index, batch_size)?;
        let query = query_builder.create_pg_query(&query_info);
        query.execute(&self.conn_pool).await.unwrap();

        BaseSinker::update_batch_monitor(&mut self.monitor, batch_size, data_size, start_time).await
    }

    async fn batch_insert(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let start_time = Instant::now();

        let tb_meta = self.meta_manager.get_tb_meta_by_row_data(&data[0]).await?;
        let query_builder = RdbQueryBuilder::new_for_pg(tb_meta);

        let (query_info, data_size) =
            query_builder.get_batch_insert_query(data, start_index, batch_size)?;
        let query = query_builder.create_pg_query(&query_info);

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

        BaseSinker::update_batch_monitor(&mut self.monitor, batch_size, data_size, start_time).await
    }

    #[allow(clippy::type_complexity)]
    fn get_insert_query<'a>(
        query_builder: &'a RdbQueryBuilder,
        tb_meta: &'a PgTbMeta,
        row_data: &'a RowData,
    ) -> Result<RdbQueryInfo<'a>, Error> {
        let mut query_info = query_builder.get_insert_query(row_data)?;

        let mut placeholder_index = query_info.cols.len() + 1;
        let after = row_data.after.as_ref().unwrap();
        let mut set_pairs = Vec::new();
        for col in tb_meta.basic.cols.iter() {
            let set_pair = format!(
                r#""{}"={}"#,
                col,
                query_builder.get_placeholder(placeholder_index, col)
            );
            set_pairs.push(set_pair);
            query_info.cols.push(col.clone());
            query_info.binds.push(after.get(col));
            placeholder_index += 1;
        }

        query_info.sql = format!(
            "{} ON CONFLICT ({}) DO UPDATE SET {}",
            query_info.sql,
            SqlUtil::escape_cols(&tb_meta.basic.id_cols, &DbType::Pg).join(","),
            set_pairs.join(",")
        );
        Ok(query_info)
    }
}
