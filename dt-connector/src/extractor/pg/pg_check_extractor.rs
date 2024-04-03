use std::collections::HashMap;

use async_trait::async_trait;

use futures::TryStreamExt;

use sqlx::{Pool, Postgres};

use dt_common::{error::Error, log_info};

use dt_common::meta::{
    adaptor::pg_col_value_convertor::PgColValueConvertor,
    col_value::ColValue,
    pg::{pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
    position::Position,
    row_data::RowData,
    row_type::RowType,
};

use crate::{
    check_log::{check_log::CheckLog, log_type::LogType},
    extractor::{base_check_extractor::BaseCheckExtractor, base_extractor::BaseExtractor},
    rdb_query_builder::RdbQueryBuilder,
    BatchCheckExtractor, Extractor,
};

pub struct PgCheckExtractor {
    pub base_extractor: BaseExtractor,
    pub conn_pool: Pool<Postgres>,
    pub meta_manager: PgMetaManager,
    pub check_log_dir: String,
    pub batch_size: usize,
}

#[async_trait]
impl Extractor for PgCheckExtractor {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!("PgCheckExtractor starts");
        let base_check_extractor = BaseCheckExtractor {
            check_log_dir: self.check_log_dir.clone(),
            batch_size: self.batch_size,
        };
        base_check_extractor.extract(self).await.unwrap();
        self.base_extractor.wait_task_finish().await
    }
}

#[async_trait]
impl BatchCheckExtractor for PgCheckExtractor {
    async fn batch_extract(&mut self, check_logs: &[CheckLog]) -> Result<(), Error> {
        let log_type = &check_logs[0].log_type;
        let tb_meta = self
            .meta_manager
            .get_tb_meta(&check_logs[0].schema, &check_logs[0].tb)
            .await?
            .to_owned();
        let check_row_datas = self.build_check_row_datas(check_logs, &tb_meta)?;

        let query_builder = RdbQueryBuilder::new_for_pg(&tb_meta);
        let query_info = if check_logs.len() == 1 {
            query_builder.get_select_query(&check_row_datas[0])?
        } else {
            query_builder.get_batch_select_query(&check_row_datas, 0, check_row_datas.len())?
        };
        let query = query_builder.create_pg_query(&query_info);

        let mut rows = query.fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let mut row_data = RowData::from_pg_row(&row, &tb_meta);

            if log_type == &LogType::Diff {
                row_data.row_type = RowType::Update;
                row_data.before = row_data.after.clone();
            }

            self.base_extractor
                .push_row(row_data, Position::None)
                .await
                .unwrap();
        }

        Ok(())
    }
}

impl PgCheckExtractor {
    fn build_check_row_datas(
        &mut self,
        check_logs: &[CheckLog],
        tb_meta: &PgTbMeta,
    ) -> Result<Vec<RowData>, Error> {
        let mut result = Vec::new();
        for check_log in check_logs.iter() {
            let mut after = HashMap::new();
            for (col, value) in check_log.id_col_values.iter() {
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
