use std::{
    collections::HashMap,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use concurrent_queue::ConcurrentQueue;
use futures::TryStreamExt;
use sqlx::{mysql::MySqlRow, MySql, Pool, Row};

use crate::{
    error::Error,
    ext::sqlx_ext::SqlxExt,
    meta::{
        col_meta::ColMeta, col_type::ColType, col_value::ColValue, db_meta_manager::DbMetaManager,
        row_data::RowData, row_type::RowType, tb_meta::TbMeta,
    },
};

pub struct MysqlSnapshotExtractor<'a> {
    pub conn_pool: &'a Pool<MySql>,
    pub db_meta_manager: DbMetaManager<'a>,
    pub buffer: &'a ConcurrentQueue<RowData>,
    pub db: String,
    pub tb: String,
    pub shut_down: &'a AtomicBool,
}

impl MysqlSnapshotExtractor<'_> {
    pub async fn extract(&mut self) -> Result<(), Error> {
        let tb_meta = self.db_meta_manager.get_tb_meta(&self.db, &self.tb).await?;

        if let Some(order_col) = &tb_meta.order_col {
            let order_col_meta = tb_meta.col_meta_map.get(order_col);
            self.extract_by_slices(&tb_meta, order_col_meta.unwrap(), ColValue::None)
                .await?;
        } else {
            self.extract_all(&tb_meta).await?;
        }

        while !self.buffer.is_empty() {
            async_std::task::sleep(Duration::from_millis(1)).await;
        }
        self.shut_down.store(true, Ordering::Release);
        Ok(())
    }

    async fn extract_all(&self, tb_meta: &TbMeta) -> Result<(), Error> {
        let sql = format!("SELECT * FROM {}.{}", self.db, self.tb);
        let mut rows = sqlx::query(&sql).fetch(self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            self.push_row_to_buffer(&row, tb_meta).await.unwrap();
        }
        Ok(())
    }

    async fn extract_by_slices(
        &self,
        tb_meta: &TbMeta,
        order_col_meta: &ColMeta,
        init_start_value: ColValue,
    ) -> Result<(), Error> {
        let mut start_value = init_start_value;
        let sql1 = format!(
            "SELECT * FROM {}.{} order by {} ASC LIMIT {}",
            self.db,
            self.tb,
            order_col_meta.name,
            self.buffer.capacity().unwrap()
        );
        let sql2 = format!(
            "SELECT * FROM {}.{} WHERE {} > ? order by {} ASC LIMIT {}",
            self.db,
            self.tb,
            order_col_meta.name,
            order_col_meta.name,
            self.buffer.capacity().unwrap()
        );

        loop {
            let query = if let ColValue::None = start_value {
                sqlx::query(&sql1)
            } else {
                sqlx::query(&sql2).bind_col_value(Some(start_value.clone()))
            };

            let mut rows = query.fetch(self.conn_pool);
            let mut count = 0usize;
            while let Some(row) = rows.try_next().await.unwrap() {
                self.push_row_to_buffer(&row, tb_meta).await.unwrap();
                start_value = Self::get_col_value(&row, order_col_meta).unwrap();
                count += 1;
            }

            // all data extracted
            if count < self.buffer.capacity().unwrap() {
                break;
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    async fn get_min_order_col_value(&self, col_meta: &ColMeta) -> Result<ColValue, Error> {
        let sql = format!(
            "SELECT MIN({}) AS {} FROM {}.{}",
            col_meta.name, col_meta.name, self.db, self.tb
        );
        let mut rows = sqlx::query(&sql).fetch(self.conn_pool);
        if let Some(row) = rows.try_next().await? {
            return Self::get_col_value(&row, &col_meta);
        }
        Ok(ColValue::None)
    }

    async fn push_row_to_buffer(&self, row: &MySqlRow, tb_meta: &TbMeta) -> Result<(), Error> {
        let mut after = HashMap::new();
        for (col_name, col_meta) in &tb_meta.col_meta_map {
            let col_val = Self::get_col_value(row, &col_meta)?;
            after.insert(col_name.to_string(), col_val);
        }

        while self.buffer.is_full() {
            async_std::task::sleep(Duration::from_millis(1)).await;
        }

        let _ = self.buffer.push(RowData {
            db: tb_meta.db.clone(),
            tb: tb_meta.tb.clone(),
            before: None,
            after: Some(after),
            row_type: RowType::Insert,
        });
        Ok(())
    }

    fn get_col_value(row: &MySqlRow, col_meta: &ColMeta) -> Result<ColValue, Error> {
        let col_name: &str = col_meta.name.as_ref();
        let value: Option<Vec<u8>> = row.get_unchecked(col_name);
        if let None = value {
            return Ok(ColValue::None);
        }

        match col_meta.typee {
            ColType::Tiny => {
                let value: i8 = row.try_get(col_name)?;
                return Ok(ColValue::Tiny(value));
            }
            ColType::UnsignedTiny => {
                let value: u8 = row.try_get(col_name)?;
                return Ok(ColValue::UnsignedTiny(value));
            }
            ColType::Short => {
                let value: i16 = row.try_get(col_name)?;
                return Ok(ColValue::Short(value));
            }
            ColType::UnsignedShort => {
                let value: u16 = row.try_get(col_name)?;
                return Ok(ColValue::UnsignedShort(value));
            }
            ColType::Long => {
                let value: i32 = row.try_get(col_name)?;
                return Ok(ColValue::Long(value));
            }
            ColType::UnsignedLong => {
                let value: u32 = row.try_get(col_name)?;
                return Ok(ColValue::UnsignedLong(value));
            }
            ColType::LongLong => {
                let value: i64 = row.try_get(col_name)?;
                return Ok(ColValue::LongLong(value));
            }
            ColType::UnsignedLongLong => {
                let value: u64 = row.try_get(col_name)?;
                return Ok(ColValue::UnsignedLongLong(value));
            }
            ColType::Float => {
                let value: f32 = row.try_get(col_name)?;
                return Ok(ColValue::Float(value));
            }
            ColType::Double => {
                let value: f64 = row.try_get(col_name)?;
                return Ok(ColValue::Double(value));
            }
            ColType::Decimal => {
                let value: String = row.get_unchecked(col_name);
                return Ok(ColValue::Decimal(value));
            }
            ColType::Time => {
                let value: Vec<u8> = row.get_unchecked(col_name);
                return ColValue::parse_time(value);
            }
            ColType::Date => {
                let value: Vec<u8> = row.get_unchecked(col_name);
                return ColValue::parse_date(value);
            }
            ColType::DateTime => {
                let value: Vec<u8> = row.get_unchecked(col_name);
                return ColValue::parse_datetime(value);
            }
            ColType::Timestamp {
                timezone_diff_utc_seconds: _,
            } => {
                let value: Vec<u8> = row.get_unchecked(col_name);
                return ColValue::parse_timestamp(value);
            }
            ColType::Year => {
                let value: u16 = row.try_get(col_name)?;
                return Ok(ColValue::Year(value));
            }
            ColType::String {
                length: _,
                charset: _,
            } => {
                let value: Vec<u8> = row.try_get(col_name)?;
                return Ok(ColValue::Blob(value));
            }
            ColType::Binary { length: _ } => {
                let value: Vec<u8> = row.try_get(col_name)?;
                return Ok(ColValue::Blob(value));
            }
            ColType::VarBinary { length: _ } => {
                let value: Vec<u8> = row.try_get(col_name)?;
                return Ok(ColValue::Blob(value));
            }
            ColType::Blob => {
                let value: Vec<u8> = row.try_get(col_name)?;
                return Ok(ColValue::Blob(value));
            }
            ColType::Bit => {
                let value: u64 = row.try_get(col_name)?;
                return Ok(ColValue::Bit(value as u64));
            }
            ColType::Set => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::Set2(value));
            }
            ColType::Enum => {
                let value: String = row.try_get(col_name)?;
                return Ok(ColValue::Enum2(value));
            }
            ColType::Json => {
                let value: Vec<u8> = row.get_unchecked(col_name);
                return Ok(ColValue::Json(value));
            }
            _ => {}
        }
        Ok(ColValue::None)
    }
}
