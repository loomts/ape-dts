use std::{
    collections::HashMap,
    sync::atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use futures::TryStreamExt;
use log::info;
use sqlx::{postgres::PgRow, Pool, Postgres};

use crate::{
    error::Error,
    meta::{
        col_value::ColValue,
        pg::{pg_col_type::PgColType, pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
        row_data::RowData,
        row_type::RowType,
    },
    task::task_util::TaskUtil,
    traits::{sqlx_ext::SqlxExt, traits::Extractor},
};

use super::pg_col_value_convertor::PgColValueConvertor;

pub struct PgSnapshotExtractor<'a> {
    pub conn_pool: Pool<Postgres>,
    pub meta_manager: PgMetaManager,
    pub buffer: &'a ConcurrentQueue<RowData>,
    pub slice_size: usize,
    pub schema: String,
    pub tb: String,
    pub shut_down: &'a AtomicBool,
}

#[async_trait]
impl Extractor for PgSnapshotExtractor<'_> {
    async fn extract(&mut self) -> Result<(), Error> {
        self.extract_internal().await
    }
}

impl PgSnapshotExtractor<'_> {
    pub async fn extract_internal(&mut self) -> Result<(), Error> {
        let tb_meta = self
            .meta_manager
            .get_tb_meta(&self.schema, &self.tb)
            .await?;

        if let Some(order_col) = &tb_meta.order_col {
            let order_col_type = tb_meta.col_type_map.get(order_col);
            self.extract_by_slices(
                &tb_meta,
                &order_col,
                order_col_type.unwrap(),
                ColValue::None,
            )
            .await?;
        } else {
            self.extract_all(&tb_meta).await?;
        }

        // wait all data to be transfered
        while !self.buffer.is_empty() {
            TaskUtil::sleep_millis(1).await;
        }

        self.shut_down.store(true, Ordering::Release);
        Ok(())
    }

    async fn extract_all(&mut self, tb_meta: &PgTbMeta) -> Result<(), Error> {
        info!(
            "start extracting data from {}.{} without slices",
            self.schema, self.tb
        );

        let mut all_count = 0;
        let sql = format!("SELECT * FROM {}.{}", self.schema, self.tb);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            self.push_row_to_buffer(&row, tb_meta).await.unwrap();
            all_count += 1;
        }

        info!(
            "end extracting data from {}.{}, all count: {}",
            self.schema, self.tb, all_count
        );
        Ok(())
    }

    async fn extract_by_slices(
        &mut self,
        tb_meta: &PgTbMeta,
        order_col_name: &str,
        order_col_type: &PgColType,
        init_start_value: ColValue,
    ) -> Result<(), Error> {
        info!(
            "start extracting data from {}.{} by slices",
            self.schema, self.tb
        );

        let mut all_count = 0;
        let mut start_value = init_start_value;
        let sql1 = format!(
            "SELECT * FROM {}.{} ORDER BY {} ASC LIMIT {}",
            self.schema, self.tb, order_col_name, self.slice_size
        );
        let sql2 = format!(
            "SELECT * FROM {}.{} WHERE {} > $1 ORDER BY {} ASC LIMIT {}",
            self.schema, self.tb, order_col_name, order_col_name, self.slice_size
        );

        loop {
            let start_value_for_bind = start_value.clone();
            let query = if let ColValue::None = start_value {
                sqlx::query(&sql1)
            } else {
                sqlx::query(&sql2).bind_col_value(Some(&start_value_for_bind))
            };

            let mut rows = query.fetch(&self.conn_pool);
            let mut slice_count = 0usize;
            while let Some(row) = rows.try_next().await.unwrap() {
                self.push_row_to_buffer(&row, tb_meta).await.unwrap();
                start_value =
                    PgColValueConvertor::from_query(&row, order_col_name, order_col_type).unwrap();
                slice_count += 1;
                all_count += 1;
            }

            // all data extracted
            if slice_count < self.slice_size {
                break;
            }
        }

        info!(
            "end extracting data from {}.{}, all count: {}",
            self.schema, self.tb, all_count
        );
        Ok(())
    }

    async fn push_row_to_buffer(&mut self, row: &PgRow, tb_meta: &PgTbMeta) -> Result<(), Error> {
        let mut after = HashMap::new();
        for (col_name, col_type) in &tb_meta.col_type_map {
            let col_val = PgColValueConvertor::from_query(row, &col_name, &col_type)?;
            after.insert(col_name.to_string(), col_val);
        }

        while self.buffer.is_full() {
            TaskUtil::sleep_millis(1).await;
        }

        let row_data = RowData {
            db: tb_meta.schema.clone(),
            tb: tb_meta.tb.clone(),
            before: None,
            after: Some(after),
            row_type: RowType::Insert,
            position: "".to_string(),
        };
        let _ = self.buffer.push(row_data);
        Ok(())
    }
}
