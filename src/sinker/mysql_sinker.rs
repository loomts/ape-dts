use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use concurrent_queue::ConcurrentQueue;
use sqlx::{MySql, Pool};

use crate::{
    error::Error,
    ext::sqlx_ext::SqlxExt,
    meta::{db_meta_manager::DbMetaManager, row_data::RowData, row_type::RowType, tb_meta::TbMeta},
};

use super::router::Router;

pub struct MysqlSinker<'a> {
    pub conn_pool: &'a Pool<MySql>,
    pub db_meta_manager: &'a DbMetaManager<'a>,
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
            query = query.bind_col_value(after.remove(col_name).unwrap());
        }
        query.execute(self.conn_pool).await?;
        Ok(())
    }

    pub async fn delete(&mut self, row_data: RowData) -> Result<(), Error> {
        let tb_meta = self.get_tb_meta(&row_data).await?;
        let mut where_col_values = Vec::new();
        for _ in tb_meta.where_cols.iter() {
            where_col_values.push("?");
        }

        let delete_sql = format!(
            "DELETE FROM {}.{} WHERE ({}) IN ({}) LIMIT 1",
            tb_meta.db,
            tb_meta.tb,
            tb_meta.where_cols.join(","),
            where_col_values.join(",")
        );

        let mut before = row_data.before.unwrap();
        let mut query = sqlx::query(&delete_sql);
        for col_name in tb_meta.where_cols.iter() {
            query = query.bind_col_value(before.remove(col_name).unwrap());
        }
        query.execute(self.conn_pool).await?;
        Ok(())
    }

    pub async fn update(&mut self, row_data: RowData) -> Result<(), Error> {
        let tb_meta = self.get_tb_meta(&row_data).await?;
        let mut before = row_data.before.unwrap();
        let mut after = row_data.after.unwrap();

        let mut where_col_values = Vec::new();
        for _ in tb_meta.where_cols.iter() {
            where_col_values.push("?");
        }

        let mut set_cols = Vec::new();
        let mut set_pairs = Vec::new();
        for (col_name, _) in after.iter() {
            set_cols.push(col_name.clone());
            set_pairs.push(format!("{}=?", col_name));
        }

        let sql = format!(
            "UPDATE {}.{} SET {} WHERE ({}) IN ({}) LIMIT 1",
            tb_meta.db,
            tb_meta.tb,
            set_pairs.join(","),
            tb_meta.where_cols.join(","),
            where_col_values.join(",")
        );

        let mut query = sqlx::query(&sql);
        for col_name in set_cols {
            query = query.bind_col_value(after.remove(&col_name).unwrap());
        }
        for col_name in tb_meta.where_cols.iter() {
            query = query.bind_col_value(before.remove(col_name).unwrap());
        }

        query.execute(self.conn_pool).await?;
        Ok(())
    }

    async fn get_tb_meta(&mut self, row_data: &RowData) -> Result<TbMeta, Error> {
        let (db, tb) = self.router.get_route(&row_data.db, &row_data.tb);
        let tb_meta = self.db_meta_manager.get_tb_meta(&db, &tb).await?;
        return Ok(tb_meta);
    }
}
