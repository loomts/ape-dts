use std::collections::HashMap;

use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::{mysql::MySqlRow, MySql, Pool};

use crate::{
    adaptor::{mysql_col_value_convertor::MysqlColValueConvertor, sqlx_ext::SqlxMysqlExt},
    common::sql_util::SqlUtil,
    error::Error,
    meta::{
        mysql::{mysql_meta_manager::MysqlMetaManager, mysql_tb_meta::MysqlTbMeta},
        row_data::RowData,
        row_type::RowType,
    },
    sinker::{rdb_router::RdbRouter, sinker_util::SinkerUtil},
    traits::Sinker,
};

#[derive(Clone)]
pub struct MysqlChecker {
    pub conn_pool: Pool<MySql>,
    pub meta_manager: MysqlMetaManager,
    pub router: RdbRouter,
    pub batch_size: usize,
}

#[async_trait]
impl Sinker for MysqlChecker {
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

impl MysqlChecker {
    async fn serial_check(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        for row_data_src in data.iter() {
            let tb_meta = self.get_tb_meta(&row_data_src).await?;
            let sql_util = SqlUtil::new_for_mysql(&tb_meta);

            let (sql, _cols, binds) = sql_util.get_select_query(row_data_src)?;
            let mut query = sqlx::query(&sql);
            for i in 0..binds.len() {
                query = query.bind_col_value(binds[i]);
            }

            let mut rows = query.fetch(&self.conn_pool);
            if let Some(row) = rows.try_next().await.unwrap() {
                let row_data_dst = Self::build_row_data(&row, &tb_meta)?;
                if !SinkerUtil::compare_row_data(row_data_src, &row_data_dst) {
                    SinkerUtil::log_diff(&row_data_src, &tb_meta.where_cols);
                }
            } else {
                SinkerUtil::log_miss(&row_data_src, &tb_meta.where_cols);
            }
        }
        Ok(())
    }

    async fn batch_check(&mut self, data: Vec<RowData>) -> Result<(), Error> {
        let all_count = data.len();
        let mut sinked_count = 0;
        let tb_meta = self.get_tb_meta(&data[0]).await?;
        let sql_util = SqlUtil::new_for_mysql(&tb_meta);

        loop {
            let mut batch_size = self.batch_size;
            if all_count - sinked_count < batch_size {
                batch_size = all_count - sinked_count;
            }

            // build fetch dst sql
            let (sql, _cols, binds) =
                sql_util.get_batch_select_query(&data, sinked_count, batch_size)?;
            let mut query = sqlx::query(&sql);
            for i in 0..binds.len() {
                query = query.bind_col_value(binds[i]);
            }

            // fetch dst
            let mut dst_row_data_map = HashMap::new();
            let mut rows = query.fetch(&self.conn_pool);
            while let Some(row) = rows.try_next().await.unwrap() {
                let row_data = Self::build_row_data(&row, &tb_meta)?;
                let hash_code = row_data.get_hash_code(&tb_meta.where_cols);
                dst_row_data_map.insert(hash_code, row_data);
            }

            SinkerUtil::batch_compare_row_datas(
                &data,
                &dst_row_data_map,
                &tb_meta.where_cols,
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
    async fn get_tb_meta(&mut self, row_data: &RowData) -> Result<MysqlTbMeta, Error> {
        let (db, tb) = self.router.get_route(&row_data.db, &row_data.tb);
        let tb_meta = self.meta_manager.get_tb_meta(&db, &tb).await?;
        return Ok(tb_meta);
    }

    fn build_row_data(row: &MySqlRow, tb_meta: &MysqlTbMeta) -> Result<RowData, Error> {
        let mut after = HashMap::new();
        for (col, col_meta) in &tb_meta.col_meta_map {
            let col_val = MysqlColValueConvertor::from_query(row, &col_meta)?;
            after.insert(col.to_string(), col_val);
        }

        let row_data = RowData {
            db: tb_meta.db.clone(),
            tb: tb_meta.tb.clone(),
            before: None,
            after: Some(after),
            row_type: RowType::Insert,
            current_position: "".to_string(),
            checkpoint_position: "".to_string(),
        };
        Ok(row_data)
    }
}
