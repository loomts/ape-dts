use std::{
    collections::HashMap,
    sync::atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;

use futures::TryStreamExt;
use log::info;
use sqlx::{Pool, Postgres};

use crate::{
    adaptor::pg_col_value_convertor::PgColValueConvertor,
    common::{check_log::CheckLog, log_reader::LogReader, sql_util::SqlUtil},
    error::Error,
    extractor::extractor_util::ExtractorUtil,
    meta::{pg::pg_meta_manager::PgMetaManager, row_data::RowData},
    task::task_util::TaskUtil,
    traits::Extractor,
};

pub struct PgCheckExtractor<'a> {
    pub conn_pool: Pool<Postgres>,
    pub meta_manager: PgMetaManager,
    pub check_log_dir: String,
    pub buffer: &'a ConcurrentQueue<RowData>,
    pub slice_size: usize,
    pub shut_down: &'a AtomicBool,
}

#[async_trait]
impl Extractor for PgCheckExtractor<'_> {
    async fn extract(&mut self) -> Result<(), Error> {
        info!(
            "PgSnapshotExtractor starts, check_log_dir: {}",
            self.check_log_dir
        );
        self.extract_internal().await
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl PgCheckExtractor<'_> {
    async fn extract_internal(&mut self) -> Result<(), Error> {
        let mut log_reader = LogReader::new(&self.check_log_dir);
        while let Some(log) = log_reader.next() {
            if log.trim().is_empty() {
                continue;
            }

            let check_log = CheckLog::from_str(&log);
            let tb_meta = self
                .meta_manager
                .get_tb_meta(&check_log.schema, &check_log.tb)
                .await?;

            let mut after = HashMap::new();
            for i in 0..check_log.cols.len() {
                let col = &check_log.cols[i];
                let value = &check_log.col_values[i];
                let col_type = tb_meta.col_type_map.get(col).unwrap();
                let col_value =
                    PgColValueConvertor::from_str(col_type, value, &mut self.meta_manager)?;
                after.insert(col.to_string(), col_value);
            }
            let check_row_data = RowData::build_insert_row_data(after, &tb_meta.basic);

            let sql_util = SqlUtil::new_for_pg(&tb_meta);
            let (sql, cols, binds) = sql_util.get_select_query(&check_row_data)?;
            let query = SqlUtil::create_pg_query(&sql, &cols, &binds, &tb_meta);

            let mut rows = query.fetch(&self.conn_pool);
            while let Some(row) = rows.try_next().await.unwrap() {
                let row_data = RowData::from_pg_row(&row, &tb_meta);
                ExtractorUtil::push_row(self.buffer, row_data)
                    .await
                    .unwrap();
            }
        }

        // wait all data to be transfered
        while !self.buffer.is_empty() {
            TaskUtil::sleep_millis(1).await;
        }

        self.shut_down.store(true, Ordering::Release);
        Ok(())
    }
}
