use std::collections::HashMap;

use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{Pool, Postgres};

use crate::{
    common::sql_util::SqlUtil,
    error::Error,
    meta::{
        pg::{pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
        row_data::RowData,
    },
    sinker::{rdb_router::RdbRouter, sinker_util::SinkerUtil},
    traits::Sinker,
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
    async fn sink(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        if data.len() == 0 {
            return Ok(());
        }
        self.serial_check(data).await
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
        self.batch_check(data).await
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
                if !SinkerUtil::compare_row_data(row_data_src, &row_data_dst) {
                    SinkerUtil::log_diff(&row_data_src, &tb_meta.basic);
                }
            } else {
                SinkerUtil::log_miss(&row_data_src, &tb_meta.basic);
            }
        }
        Ok(())
    }

    async fn batch_check(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        let all_count = data.len();
        let mut sinked_count = 0;
        let tb_meta = self.get_tb_meta(&data[0]).await?;
        let sql_util = SqlUtil::new_for_pg(&tb_meta);

        loop {
            let mut batch_size = self.batch_size;
            if all_count - sinked_count < batch_size {
                batch_size = all_count - sinked_count;
            }

            // build fetch dst sql
            let (sql, cols, binds) =
                sql_util.get_batch_select_query(&data, sinked_count, batch_size)?;
            let query = SqlUtil::create_pg_query(&sql, &cols, &binds, &tb_meta);

            // fetch dst
            let mut dst_row_data_map = HashMap::new();
            let mut rows = query.fetch(&self.conn_pool);
            while let Some(row) = rows.try_next().await.unwrap() {
                let row_data = RowData::from_pg_row(&row, &tb_meta);
                let hash_code = row_data.get_hash_code(&tb_meta.basic);
                dst_row_data_map.insert(hash_code, row_data);
            }

            SinkerUtil::batch_compare_row_datas(
                &data,
                &dst_row_data_map,
                &tb_meta.basic,
                sinked_count,
                batch_size,
            );

            sinked_count += batch_size;
            if sinked_count == all_count {
                break;
            }
        }
        Ok(())
    }

    #[inline(always)]
    async fn get_tb_meta(&mut self, row_data: &RowData) -> Result<PgTbMeta, Error> {
        SinkerUtil::get_pg_tb_meta(&mut self.meta_manager, &mut self.router, row_data).await
    }
}
