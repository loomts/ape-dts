use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc, Mutex},
    time::Instant,
};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::{error::Error, log_info, monitor::monitor::Monitor};
use dt_meta::{
    adaptor::mysql_col_value_convertor::MysqlColValueConvertor,
    col_value::ColValue,
    dt_data::DtItem,
    mysql::{mysql_meta_manager::MysqlMetaManager, mysql_tb_meta::MysqlTbMeta},
    position::Position,
    row_data::RowData,
    row_type::RowType,
};
use futures::TryStreamExt;
use sqlx::{MySql, Pool};

use crate::{
    check_log::{check_log::CheckLog, log_type::LogType},
    extractor::{base_check_extractor::BaseCheckExtractor, base_extractor::BaseExtractor},
    rdb_query_builder::RdbQueryBuilder,
    rdb_router::RdbRouter,
    BatchCheckExtractor, Extractor,
};

pub struct MysqlCheckExtractor {
    pub conn_pool: Pool<MySql>,
    pub meta_manager: MysqlMetaManager,
    pub check_log_dir: String,
    pub buffer: Arc<ConcurrentQueue<DtItem>>,
    pub batch_size: usize,
    pub shut_down: Arc<AtomicBool>,
    pub router: RdbRouter,
    pub monitor: Arc<Mutex<Monitor>>,
}

#[async_trait]
impl Extractor for MysqlCheckExtractor {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!(
            "MysqlCheckExtractor starts, check_log_dir: {}",
            self.check_log_dir
        );

        let mut base_check_extractor = BaseCheckExtractor {
            check_log_dir: self.check_log_dir.clone(),
            buffer: self.buffer.clone(),
            batch_size: self.batch_size,
            shut_down: self.shut_down.clone(),
        };

        base_check_extractor.extract(self).await
    }
}

#[async_trait]
impl BatchCheckExtractor for MysqlCheckExtractor {
    async fn batch_extract(&mut self, check_logs: &[CheckLog]) -> Result<(), Error> {
        let mut last_monitored_time = Instant::now();
        let monitor_count_window = self.monitor.lock().unwrap().count_window;
        let monitor_time_window_secs = self.monitor.lock().unwrap().time_window_secs as u64;
        let mut monitored_count = 0;
        let mut extracted_count = 0;

        if check_logs.is_empty() {
            return Ok(());
        }

        let log_type = &check_logs[0].log_type;
        let tb_meta = self
            .meta_manager
            .get_tb_meta(&check_logs[0].schema, &check_logs[0].tb)
            .await?;
        let check_row_datas = Self::build_check_row_datas(check_logs, &tb_meta)?;

        let query_builder = RdbQueryBuilder::new_for_mysql(&tb_meta);
        let (sql, cols, binds) = if check_logs.len() == 1 {
            query_builder.get_select_query(&check_row_datas[0])?
        } else {
            query_builder.get_batch_select_query(&check_row_datas, 0, check_row_datas.len())?
        };
        let query = query_builder.create_mysql_query(&sql, &cols, &binds);

        let mut rows = query.fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let mut row_data = RowData::from_mysql_row(&row, &tb_meta);

            if log_type == &LogType::Diff {
                row_data.row_type = RowType::Update;
                row_data.before = row_data.after.clone();
            }

            BaseExtractor::push_row(
                self.buffer.as_ref(),
                row_data,
                Position::None,
                Some(&self.router),
            )
            .await
            .unwrap();
            extracted_count += 1;

            (last_monitored_time, monitored_count) = BaseExtractor::update_monitor(
                &mut self.monitor,
                extracted_count,
                monitored_count,
                monitor_count_window,
                monitor_time_window_secs,
                last_monitored_time,
            );
        }
        Ok(())
    }
}

impl MysqlCheckExtractor {
    fn build_check_row_datas(
        check_logs: &[CheckLog],
        tb_meta: &MysqlTbMeta,
    ) -> Result<Vec<RowData>, Error> {
        let mut result = Vec::new();
        for check_log in check_logs.iter() {
            let mut after = HashMap::new();
            for i in 0..check_log.cols.len() {
                let col = &check_log.cols[i];
                let value = &check_log.col_values[i];
                let col_type = tb_meta.col_type_map.get(col).unwrap();
                let col_value = if let Some(str) = value {
                    MysqlColValueConvertor::from_str(col_type, str)?
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
