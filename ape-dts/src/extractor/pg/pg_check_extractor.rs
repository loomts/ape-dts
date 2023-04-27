use std::{collections::HashMap, sync::atomic::AtomicBool};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;

use futures::TryStreamExt;

use sqlx::{Pool, Postgres};

use crate::{
    adaptor::pg_col_value_convertor::PgColValueConvertor,
    common::sql_util::SqlUtil,
    error::Error,
    extractor::{base_check_extractor::BaseCheckExtractor, base_extractor::BaseExtractor},
    info,
    log::{check_log::CheckLog, log_type::LogType},
    meta::{
        col_value::ColValue,
        dt_data::DtData,
        pg::{pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
        row_data::RowData,
        row_type::RowType,
    },
    traits::{BatchCheckExtractor, Extractor},
};

pub struct PgCheckExtractor<'a> {
    pub conn_pool: Pool<Postgres>,
    pub meta_manager: PgMetaManager,
    pub check_log_dir: String,
    pub buffer: &'a ConcurrentQueue<DtData>,
    pub batch_size: usize,
    pub shut_down: &'a AtomicBool,
}

#[async_trait]
impl Extractor for PgCheckExtractor<'_> {
    async fn extract(&mut self) -> Result<(), Error> {
        info!(
            "PgCheckExtractor starts, check_log_dir: {}",
            self.check_log_dir
        );

        let mut base_check_extractor = BaseCheckExtractor {
            check_log_dir: self.check_log_dir.clone(),
            buffer: &self.buffer,
            batch_size: self.batch_size,
            shut_down: &self.shut_down,
        };

        let mut batch_extractor: Box<&mut (dyn BatchCheckExtractor + Send)> = Box::new(self);
        base_check_extractor.extract(&mut batch_extractor).await
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[async_trait]
impl BatchCheckExtractor for PgCheckExtractor<'_> {
    async fn batch_extract(&mut self, check_logs: &Vec<CheckLog>) -> Result<(), Error> {
        if check_logs.len() == 0 {
            return Ok(());
        }

        let log_type = &check_logs[0].log_type;
        let tb_meta = self
            .meta_manager
            .get_tb_meta(&check_logs[0].schema, &check_logs[0].tb)
            .await?;
        let check_row_datas = self.build_check_row_datas(check_logs, &tb_meta)?;

        let sql_util = SqlUtil::new_for_pg(&tb_meta);
        let (sql, cols, binds) = if check_logs.len() == 1 {
            sql_util.get_select_query(&check_row_datas[0])?
        } else {
            sql_util.get_batch_select_query(&check_row_datas, 0, check_row_datas.len())?
        };
        let query = SqlUtil::create_pg_query(&sql, &cols, &binds, &tb_meta);

        let mut rows = query.fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let mut row_data = RowData::from_pg_row(&row, &tb_meta);

            if log_type == &LogType::Diff {
                row_data.row_type = RowType::Update;
                row_data.before = row_data.after.clone();
            }

            BaseExtractor::push_row(self.buffer, row_data)
                .await
                .unwrap();
        }

        Ok(())
    }
}

impl PgCheckExtractor<'_> {
    fn build_check_row_datas(
        &mut self,
        check_logs: &Vec<CheckLog>,
        tb_meta: &PgTbMeta,
    ) -> Result<Vec<RowData>, Error> {
        let mut result = Vec::new();
        for check_log in check_logs.iter() {
            let mut after = HashMap::new();
            for i in 0..check_log.cols.len() {
                let col = &check_log.cols[i];
                let value = &check_log.col_values[i];
                let col_type = tb_meta.col_type_map.get(col).unwrap();
                let col_value = if let Some(str) = value {
                    PgColValueConvertor::from_str(col_type, str, &mut self.meta_manager)?
                } else {
                    ColValue::None
                };
                after.insert(col.to_string(), col_value);
            }
            let check_row_data = RowData::build_insert_row_data(after, &tb_meta.basic);
            result.push(check_row_data);
        }
        Ok(result)
    }
}
