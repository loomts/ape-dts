use std::{collections::HashMap, sync::atomic::AtomicBool};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;

use log::info;
use sqlx::{Pool, Postgres};

use crate::{
    adaptor::pg_col_value_convertor::PgColValueConvertor,
    common::{check_log_line::CheckLogLine, log_reader::LogReader},
    error::Error,
    meta::{
        col_value::ColValue,
        pg::{pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
        row_data::RowData,
        row_type::RowType,
    },
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

            let check_log_line = CheckLogLine::from_string(log);
            let tb_meta = self
                .meta_manager
                .get_tb_meta(&check_log_line.schema, &check_log_line.tb)
                .await?;

            let mut after = HashMap::new();
            for i in 0..check_log_line.cols.len() {
                let col = &check_log_line.cols[i];
                let value = &check_log_line.col_values[i];
                let col_type = tb_meta.col_type_map.get(col).unwrap();
                let col_value =
                    PgColValueConvertor::from_str(col_type, value, &mut self.meta_manager)?;
                after.insert(col.to_string(), col_value);
            }
            self.push_row_to_buffer(after, &tb_meta).await?;
        }

        Ok(())
    }

    async fn push_row_to_buffer(
        &mut self,
        after: HashMap<String, ColValue>,
        tb_meta: &PgTbMeta,
    ) -> Result<(), Error> {
        while self.buffer.is_full() {
            TaskUtil::sleep_millis(1).await;
        }

        let row_data = RowData {
            db: tb_meta.schema.clone(),
            tb: tb_meta.tb.clone(),
            before: None,
            after: Some(after),
            row_type: RowType::Insert,
            current_position: "".to_string(),
            checkpoint_position: "".to_string(),
        };
        let _ = self.buffer.push(row_data);
        Ok(())
    }
}
