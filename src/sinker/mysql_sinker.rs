use std::collections::HashMap;

use concurrent_queue::ConcurrentQueue;

use log::info;
use sqlx::{mysql::MySqlQueryResult, MySql, Pool};

use crate::{
    error::Error,
    ext::sqlx_ext::SqlxExt,
    meta::{
        col_value::ColValue, db_meta_manager::DbMetaManager, row_data::RowData, row_type::RowType,
        tb_meta::TbMeta,
    },
};

use super::{router::Router, traits::Sinker};

use async_trait::async_trait;

pub struct MysqlSinker {
    pub conn_pool: Pool<MySql>,
    pub db_meta_manager: DbMetaManager,
    pub buffer: ConcurrentQueue<RowData>,
    pub router: Router,
}

#[async_trait]
impl Sinker for MysqlSinker {
    async fn sink(&mut self) -> Result<(), Error> {
        self.sink_internal().await
    }

    async fn accept(&self, row_data: RowData) -> Result<(), Error> {
        self.buffer.push(row_data).unwrap();
        Ok(())
    }
}

impl MysqlSinker {
    pub async fn sink_internal(&mut self) -> Result<(), Error> {
        while let Ok(mut row_data) = self.buffer.pop() {
            match row_data.row_type {
                RowType::Insert => {
                    self.insert(&mut row_data).await.unwrap();
                }

                RowType::Update => {
                    self.update(&mut row_data).await.unwrap();
                }

                RowType::Delete => {
                    self.delete(&mut row_data).await.unwrap();
                }
            }
        }

        Ok(())
    }

    async fn insert(&mut self, row_data: &mut RowData) -> Result<(), Error> {
        let tb_meta = self.get_tb_meta(&row_data).await?;
        let mut col_values = Vec::new();
        for _ in tb_meta.cols.iter() {
            col_values.push("?");
        }

        let sql = format!(
            "REPLACE INTO {}.{}({}) VALUES({})",
            tb_meta.db,
            tb_meta.tb,
            tb_meta.cols.join(","),
            col_values.join(",")
        );

        let after = row_data.after.as_ref().unwrap();
        let mut query = sqlx::query(&sql);
        for col_name in tb_meta.cols.iter() {
            query = query.bind_col_value(after.get(col_name));
        }

        let result = query.execute(&self.conn_pool).await?;
        self.check_result(result, &sql, row_data, &tb_meta).await
    }

    async fn delete(&mut self, row_data: &mut RowData) -> Result<(), Error> {
        let tb_meta = self.get_tb_meta(&row_data).await?;
        let before = row_data.before.as_ref().unwrap();

        let (where_sql, not_null_cols) = self.get_where_info(&tb_meta, &before)?;
        let sql = format!(
            "DELETE FROM {}.{} WHERE {} LIMIT 1",
            tb_meta.db, tb_meta.tb, where_sql,
        );

        let mut query = sqlx::query(&sql);
        for col_name in not_null_cols.iter() {
            query = query.bind_col_value(before.get(col_name));
        }

        let result = query.execute(&self.conn_pool).await?;
        self.check_result(result, &sql, row_data, &tb_meta).await
    }

    async fn update(&mut self, row_data: &mut RowData) -> Result<(), Error> {
        let tb_meta = self.get_tb_meta(&row_data).await?;
        let before = row_data.before.as_ref().unwrap();
        let after = row_data.after.as_ref().unwrap();

        let mut set_cols = Vec::new();
        let mut set_pairs = Vec::new();
        for (col_name, _) in after.iter() {
            set_cols.push(col_name.clone());
            set_pairs.push(format!("{}=?", col_name));
        }

        let (where_sql, not_null_cols) = self.get_where_info(&tb_meta, &before)?;
        let sql = format!(
            "UPDATE {}.{} SET {} WHERE {} LIMIT 1",
            tb_meta.db,
            tb_meta.tb,
            set_pairs.join(","),
            where_sql,
        );

        let mut query = sqlx::query(&sql);
        for col_name in set_cols.iter() {
            query = query.bind_col_value(after.get(col_name));
        }
        for col_name in not_null_cols.iter() {
            query = query.bind_col_value(before.get(col_name));
        }

        let result = query.execute(&self.conn_pool).await?;
        self.check_result(result, &sql, row_data, &tb_meta).await
    }

    fn get_where_info(
        &mut self,
        tb_meta: &TbMeta,
        col_value_map: &HashMap<String, ColValue>,
    ) -> Result<(String, Vec<String>), Error> {
        let mut where_sql = "".to_string();
        let mut not_null_cols = Vec::new();

        for col_name in tb_meta.where_cols.iter() {
            if !where_sql.is_empty() {
                where_sql += " AND";
            }

            let col_value = col_value_map.get(col_name);
            if let Some(value) = col_value {
                if *value == ColValue::None {
                    where_sql = format!("{} {} IS NULL", where_sql, col_name);
                } else {
                    where_sql = format!("{} {} = ?", where_sql, col_name);
                    not_null_cols.push(col_name.clone());
                }
            } else {
                where_sql = format!("{} {} IS NULL", where_sql, col_name);
            }
        }

        Ok((where_sql, not_null_cols))
    }

    async fn get_tb_meta(&mut self, row_data: &RowData) -> Result<TbMeta, Error> {
        let (db, tb) = self.router.get_route(&row_data.db, &row_data.tb);
        let tb_meta = self.db_meta_manager.get_tb_meta(&db, &tb).await?;
        return Ok(tb_meta);
    }

    async fn check_result(
        &self,
        result: MySqlQueryResult,
        sql: &str,
        row_data: &RowData,
        tb_meta: &TbMeta,
    ) -> Result<(), Error> {
        if result.rows_affected() != 1 {
            info!(
                "sql: {}\nrows_affected: {}\n{}",
                sql,
                result.rows_affected(),
                row_data.to_string(tb_meta)
            );
        }
        Ok(())
    }
}
