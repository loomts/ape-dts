use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc, Mutex},
    time::Instant,
};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::{error::Error, log_info, monitor::monitor::Monitor};
use dt_meta::{
    col_value::ColValue,
    dt_data::DtItem,
    mongo::{mongo_constant::MongoConstants, mongo_key::MongoKey},
    position::Position,
    row_data::RowData,
    row_type::RowType,
};

use mongodb::{
    bson::{doc, Document},
    Client,
};

use crate::{
    check_log::{check_log::CheckLog, log_type::LogType},
    extractor::{base_check_extractor::BaseCheckExtractor, base_extractor::BaseExtractor},
    rdb_router::RdbRouter,
    BatchCheckExtractor, Extractor,
};

pub struct MongoCheckExtractor {
    pub mongo_client: Client,
    pub check_log_dir: String,
    pub buffer: Arc<ConcurrentQueue<DtItem>>,
    pub batch_size: usize,
    pub shut_down: Arc<AtomicBool>,
    pub router: RdbRouter,
    pub monitor: Arc<Mutex<Monitor>>,
}

#[async_trait]
impl Extractor for MongoCheckExtractor {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!(
            "MongoCheckExtractor starts, check_log_dir: {}",
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
impl BatchCheckExtractor for MongoCheckExtractor {
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
        let schema = &check_logs[0].schema;
        let tb = &check_logs[0].tb;
        let collection = self
            .mongo_client
            .database(schema)
            .collection::<Document>(tb);

        let mut ids = Vec::new();
        for check_log in check_logs.iter() {
            // check log has only one col: _id
            if let Some(key_str) = &check_log.col_values[0] {
                let key: MongoKey = serde_json::from_str(key_str).unwrap();
                ids.push(key.to_mongo_id());
            }
        }

        let filter = doc! {
            MongoConstants::ID: {
                "$in": ids
            }
        };

        let mut cursor = collection.find(filter, None).await.unwrap();
        while cursor.advance().await.unwrap() {
            let doc = cursor.deserialize_current().unwrap();
            let mut after = HashMap::new();
            let id: String = MongoKey::from_doc(&doc).unwrap().to_string();
            after.insert(MongoConstants::ID.to_string(), ColValue::String(id));
            after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(doc));
            let mut row_data = RowData {
                schema: schema.clone(),
                tb: tb.clone(),
                row_type: RowType::Insert,
                after: Some(after),
                before: None,
            };

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
