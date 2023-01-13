use std::{
    collections::HashMap,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use concurrent_queue::ConcurrentQueue;
use sqlx::{MySql, Pool};

use crate::{
    error::Error,
    ext::sqlx_ext::SqlxExt,
    meta::{
        col_value::ColValue, db_meta_manager::DbMetaManager, row_data::RowData, row_type::RowType,
        tb_meta::TbMeta,
    },
};

use super::router::Router;

pub struct MysqlSinker<'a> {
    pub conn_pool: &'a Pool<MySql>,
    pub db_meta_manager: DbMetaManager<'a>,
    pub buffer: &'a ConcurrentQueue<RowData>,
    pub router: Router,
    pub shut_down: &'a AtomicBool,
}

impl MysqlSinker<'_> {
    pub async fn sink(&mut self) -> Result<(), Error> {
        while !self.shut_down.load(Ordering::Acquire) {
            if let Ok(row_data) = self.buffer.pop() {
                match row_data.row_type {
                    RowType::Insert => {
                        self.insert(row_data).await?;
                    }

                    RowType::Update => {
                        self.update(row_data).await?;
                    }

                    RowType::Delete => {
                        self.delete(row_data).await?;
                    }
                }
            } else {
                async_std::task::sleep(Duration::from_millis(1)).await;
            }
        }
        Ok(())
    }

    pub async fn insert(&mut self, row_data: RowData) -> Result<(), Error> {
        let tb_meta = self.get_tb_meta(&row_data).await?;
        let mut col_values = Vec::new();
        for _ in tb_meta.cols.iter() {
            col_values.push("?");
        }

        let sql = format!(
            "INSERT INTO {}.{}({}) VALUES({})",
            tb_meta.db,
            tb_meta.tb,
            tb_meta.cols.join(","),
            col_values.join(",")
        );

        let mut after = row_data.after.unwrap();
        let mut query = sqlx::query(&sql);
        for col_name in tb_meta.cols.iter() {
            query = query.bind_col_value(after.remove(col_name));
        }
        query.execute(self.conn_pool).await?;
        Ok(())
    }

    pub async fn delete(&mut self, row_data: RowData) -> Result<(), Error> {
        let tb_meta = self.get_tb_meta(&row_data).await?;
        let mut before = row_data.before.unwrap();

        let (where_sql, not_null_cols) = self.get_where_info(&tb_meta, &before)?;
        let delete_sql = format!(
            "DELETE FROM {}.{} WHERE {} LIMIT 1",
            tb_meta.db, tb_meta.tb, where_sql,
        );

        let mut query = sqlx::query(&delete_sql);
        for col_name in not_null_cols.iter() {
            query = query.bind_col_value(before.remove(col_name));
        }
        query.execute(self.conn_pool).await?;
        Ok(())
    }

    pub async fn update(&mut self, row_data: RowData) -> Result<(), Error> {
        let tb_meta = self.get_tb_meta(&row_data).await?;
        let mut before = row_data.before.unwrap();
        let mut after = row_data.after.unwrap();

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
        for col_name in set_cols {
            query = query.bind_col_value(after.remove(&col_name));
        }
        for col_name in not_null_cols.iter() {
            query = query.bind_col_value(before.remove(col_name));
        }

        query.execute(self.conn_pool).await?;
        Ok(())
    }

    fn get_where_info(
        &mut self,
        tb_meta: &TbMeta,
        where_col_values: &HashMap<String, ColValue>,
    ) -> Result<(String, Vec<String>), Error> {
        let mut where_sql = "".to_string();
        let mut not_null_cols = Vec::new();

        for col_name in tb_meta.where_cols.iter() {
            if !where_sql.is_empty() {
                where_sql += " AND";
            }

            let col_value = where_col_values.get(col_name);
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
}
