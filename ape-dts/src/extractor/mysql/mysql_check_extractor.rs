use std::{
    collections::HashMap,
    sync::atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use futures::TryStreamExt;
use log::info;
use sqlx::{mysql::MySqlRow, MySql, Pool};

use crate::{
    adaptor::{mysql_col_value_convertor::MysqlColValueConvertor, sqlx_ext::SqlxMysqlExt},
    common::{check_log_line::CheckLogLine, log_reader::LogReader, sql_util::SqlUtil},
    error::Error,
    meta::{
        mysql::{mysql_meta_manager::MysqlMetaManager, mysql_tb_meta::MysqlTbMeta},
        row_data::RowData,
        row_type::RowType,
    },
    task::task_util::TaskUtil,
    traits::Extractor,
};

pub struct MysqlCheckExtractor<'a> {
    pub conn_pool: Pool<MySql>,
    pub meta_manager: MysqlMetaManager,
    pub check_log_dir: String,
    pub buffer: &'a ConcurrentQueue<RowData>,
    pub slice_size: usize,
    pub shut_down: &'a AtomicBool,
}

#[async_trait]
impl Extractor for MysqlCheckExtractor<'_> {
    async fn extract(&mut self) -> Result<(), Error> {
        info!(
            "MysqlCheckExtractor starts, check_log_dir: {}",
            self.check_log_dir
        );
        self.extract_internal().await
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl MysqlCheckExtractor<'_> {
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
                let col_meta = tb_meta.col_meta_map.get(col).unwrap();
                let col_value = MysqlColValueConvertor::from_str(col_meta, value)?;
                after.insert(col.to_string(), col_value);
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
            // Self::push_row_to_map(&mut row_data_map, row_data);
            // count += 1;

            let sql_util = SqlUtil::new_for_mysql(&tb_meta);
            let (sql, _cols, binds) = sql_util.get_select_query(&row_data)?;
            let mut query = sqlx::query(&sql);
            for i in 0..binds.len() {
                query = query.bind_col_value(binds[i]);
            }

            let mut rows = query.fetch(&self.conn_pool);
            while let Some(row) = rows.try_next().await.unwrap() {
                self.push_row_to_buffer(&row, &tb_meta).await.unwrap();
            }
        }

        // wait all data to be transfered
        while !self.buffer.is_empty() {
            TaskUtil::sleep_millis(1).await;
        }

        self.shut_down.store(true, Ordering::Release);
        Ok(())
    }

    async fn push_row_to_buffer(
        &mut self,
        row: &MySqlRow,
        tb_meta: &MysqlTbMeta,
    ) -> Result<(), Error> {
        let mut after = HashMap::new();
        for (col_name, col_meta) in &tb_meta.col_meta_map {
            let col_val = MysqlColValueConvertor::from_query(row, &col_meta)?;
            after.insert(col_name.to_string(), col_val);
        }

        while self.buffer.is_full() {
            TaskUtil::sleep_millis(1).await;
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
        let _ = self.buffer.push(row_data);
        Ok(())
    }

    // fn push_row_to_map(map: &mut HashMap<String, Vec<RowData>>, row_data: RowData) {
    //     let full_tb = format!("{}.{}", row_data.db, row_data.tb);
    //     if let Some(vec) = map.get(&full_tb) {
    //         vec.push(row_data);
    //     } else {
    //         let mut vec = vec![row_data];
    //         map.insert(full_tb, vec);
    //     }
    // }
}
