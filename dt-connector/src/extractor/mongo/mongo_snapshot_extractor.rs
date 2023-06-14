use std::{
    collections::HashMap,
    sync::atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::{constants::MongoConstants, error::Error, log_info, utils::time_util::TimeUtil};
use dt_meta::{col_value::ColValue, dt_data::DtData, row_data::RowData, row_type::RowType};
use mongodb::{
    bson::{doc, oid::ObjectId, Document},
    options::FindOptions,
    Client,
};

use crate::{
    extractor::{base_extractor::BaseExtractor, snapshot_resumer::SnapshotResumer},
    Extractor,
};

pub struct MongoSnapshotExtractor<'a> {
    pub resumer: SnapshotResumer,
    pub buffer: &'a ConcurrentQueue<DtData>,
    pub db: String,
    pub tb: String,
    pub shut_down: &'a AtomicBool,
    pub mongo_client: Client,
}

#[async_trait]
impl Extractor for MongoSnapshotExtractor<'_> {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!(
            "MongoSnapshotExtractor starts, schema: {}, tb: {}",
            self.db,
            self.tb
        );
        self.extract_internal().await
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl MongoSnapshotExtractor<'_> {
    pub async fn extract_internal(&mut self) -> Result<(), Error> {
        log_info!("start extracting data from {}.{}", self.db, self.tb);

        let filter = if let Some(resume_value) =
            self.resumer
                .get_resume_value(&self.db, &self.tb, MongoConstants::ID)
        {
            let start_id = ObjectId::parse_str(resume_value).unwrap();
            Some(doc! {MongoConstants::ID: {"$gt": start_id}})
        } else {
            None
        };

        // order by asc
        let find_options = FindOptions::builder()
            .sort(doc! {MongoConstants::ID: 1})
            .build();

        let mut all_count = 0;
        let collection = self
            .mongo_client
            .database(&self.db)
            .collection::<Document>(&self.tb);
        let mut cursor = collection.find(filter, find_options).await.unwrap();
        while cursor.advance().await.unwrap() {
            let doc = cursor.deserialize_current().unwrap();

            let id = doc.get_object_id(MongoConstants::ID).unwrap().to_string();

            let mut after = HashMap::new();
            after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(doc));
            let row_data = RowData {
                schema: self.db.clone(),
                tb: self.tb.clone(),
                row_type: RowType::Insert,
                position: format!("{}:{}", MongoConstants::ID, id),
                after: Some(after),
                before: None,
            };

            BaseExtractor::push_row(self.buffer, row_data)
                .await
                .unwrap();
            all_count += 1;
        }

        log_info!(
            "end extracting data from {}.{}, all count: {}",
            self.db,
            self.tb,
            all_count
        );

        // wait all data to be transfered
        while !self.buffer.is_empty() {
            TimeUtil::sleep_millis(1).await;
        }

        self.shut_down.store(true, Ordering::Release);
        Ok(())
    }
}
