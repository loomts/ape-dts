use std::collections::HashMap;

use async_trait::async_trait;
use dt_common::error::Error;
use dt_meta::{
    ddl_data::DdlData,
    pg::{pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
    row_data::RowData,
};
use futures::TryStreamExt;
use sqlx::{Pool, Postgres};

use crate::{
    call_batch_fn, close_conn_pool,
    sinker::{base_checker::BaseChecker, base_sinker::BaseSinker, rdb_router::RdbRouter},
    sql_util::SqlUtil,
    Sinker,
};

#[derive(Clone)]
pub struct PgChecker {
    pub conn_pool: Pool<Postgres>,
    pub meta_manager: PgMetaManager,
    pub router: RdbRouter,
    pub batch_size: usize,
}

#[async_trait]
impl Sinker for PgChecker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> Result<(), Error> {
        if data.len() == 0 {
            return Ok(());
        }

        if !batch {
            self.serial_check(data).await.unwrap();
        } else {
            call_batch_fn!(self, data, Self::batch_check);
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

impl PgChecker {
    async fn serial_check(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        for row_data_src in data.iter() {
            let tb_meta = self.get_tb_meta(&row_data_src).await?;
            let sql_util = SqlUtil::new_for_pg(&tb_meta);

            let (sql, cols, binds) = sql_util.get_select_query(row_data_src)?;
            let query = SqlUtil::create_pg_query(&sql, &cols, &binds, &tb_meta);

            let mut rows = query.fetch(&self.conn_pool);
            if let Some(row) = rows.try_next().await.unwrap() {
                let row_data_dst = RowData::from_pg_row(&row, &tb_meta);
                if !BaseChecker::compare_row_data(row_data_src, &row_data_dst) {
                    BaseChecker::log_diff(&row_data_src, &tb_meta.basic);
                }
            } else {
                BaseChecker::log_miss(&row_data_src, &tb_meta.basic);
            }
        }
        Ok(())
    }

    async fn batch_check(
        &mut self,
        data: &mut Vec<RowData>,
        start_index: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let tb_meta = self.get_tb_meta(&data[0]).await?;
        let sql_util = SqlUtil::new_for_pg(&tb_meta);

        // build fetch dst sql
        let (sql, cols, binds) = sql_util.get_batch_select_query(&data, start_index, batch_size)?;
        let query = SqlUtil::create_pg_query(&sql, &cols, &binds, &tb_meta);

        // fetch dst
        let mut dst_row_data_map = HashMap::new();
        let mut rows = query.fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let row_data = RowData::from_pg_row(&row, &tb_meta);
            let hash_code = row_data.get_hash_code(&tb_meta.basic);
            dst_row_data_map.insert(hash_code, row_data);
        }

        BaseChecker::batch_compare_row_datas(
            &data,
            &dst_row_data_map,
            &tb_meta.basic,
            start_index,
            batch_size,
        );
        Ok(())
    }

    #[inline(always)]
    async fn get_tb_meta(&mut self, row_data: &RowData) -> Result<PgTbMeta, Error> {
        BaseSinker::get_pg_tb_meta(&mut self.meta_manager, &mut self.router, row_data).await
    }
}
