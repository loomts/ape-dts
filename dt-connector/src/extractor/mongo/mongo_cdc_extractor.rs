use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::{
    constants::MongoConstants,
    error::Error,
    log_info,
    utils::{position_util::PositionUtil, rdb_filter::RdbFilter},
};
use dt_meta::{col_value::ColValue, dt_data::DtData, row_data::RowData, row_type::RowType};
use mongodb::{
    bson::{doc, Timestamp},
    change_stream::event::{OperationType, ResumeToken},
    options::{ChangeStreamOptions, FullDocumentBeforeChangeType, FullDocumentType},
    Client,
};
use serde_json::json;

use crate::{extractor::base_extractor::BaseExtractor, Extractor};

pub struct MongoCdcExtractor {
    pub buffer: Arc<ConcurrentQueue<DtData>>,
    pub filter: RdbFilter,
    pub shut_down: Arc<AtomicBool>,
    pub resume_token: String,
    pub start_timestamp: i64,
    pub mongo_client: Client,
}

#[async_trait]
impl Extractor for MongoCdcExtractor {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!(
            "MongoCdcExtractor starts, resume_token: {} ",
            self.resume_token
        );
        self.extract_internal().await
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl MongoCdcExtractor {
    async fn extract_internal(&mut self) -> Result<(), Error> {
        let mut start_timestamp_option: Option<Timestamp> = None;
        let mut start_after: Option<ResumeToken> = None;

        if self.resume_token.is_empty() {
            start_timestamp_option = if self.start_timestamp > 0 {
                Some(Timestamp {
                    time: self.start_timestamp as u32,
                    increment: 0,
                })
            } else {
                None
            };
        } else {
            let token: ResumeToken = serde_json::from_str(&self.resume_token).unwrap();
            start_after = Some(token)
        };

        let stream_options = ChangeStreamOptions::builder()
            .start_at_operation_time(start_timestamp_option)
            .start_after(start_after)
            .full_document(Some(FullDocumentType::UpdateLookup))
            .full_document_before_change(Some(FullDocumentBeforeChangeType::WhenAvailable))
            .build();

        let mut change_stream = self.mongo_client.watch(None, stream_options).await.unwrap();
        loop {
            let result = change_stream.next_if_any().await.unwrap();
            if let Some(doc) = result {
                let resume_token = doc.id;
                let position: String = match doc.cluster_time {
                    Some(operation_time) => {
                        format!(
                            "resume_token:{},operation_time:{},timestamp:{}",
                            json!(resume_token),
                            operation_time.time,
                            PositionUtil::format_timestamp_millis(
                                operation_time.time as i64 * 1000
                            )
                        )
                    }
                    None => format!("resume_token:{}", json!(resume_token)),
                };

                let (mut db, mut tb) = (String::new(), String::new());
                if let Some(ns) = doc.ns {
                    db = ns.db.clone();
                    if let Some(coll) = ns.coll {
                        tb = coll.clone();
                    }
                }

                let mut row_type = RowType::Insert;
                let mut before = HashMap::new();
                let mut after = HashMap::new();

                match doc.operation_type {
                    OperationType::Insert => {
                        after.insert(
                            MongoConstants::DOC.to_string(),
                            ColValue::MongoDoc(doc.full_document.unwrap()),
                        );
                    }

                    OperationType::Delete => {
                        row_type = RowType::Delete;
                        before.insert(
                            MongoConstants::DOC.to_string(),
                            ColValue::MongoDoc(doc.document_key.unwrap()),
                        );
                    }

                    OperationType::Update | OperationType::Replace => {
                        row_type = RowType::Update;

                        if let Some(document) = doc.full_document {
                            let id = document.get_object_id(MongoConstants::ID).unwrap();

                            let before_doc = doc! {MongoConstants::ID: id};
                            let after_doc = document;

                            before.insert(
                                MongoConstants::DOC.to_string(),
                                ColValue::MongoDoc(before_doc),
                            );
                            after.insert(
                                MongoConstants::DOC.to_string(),
                                ColValue::MongoDoc(after_doc),
                            );
                        }
                    }

                    _ => {}
                }

                let row_data = RowData {
                    schema: db,
                    tb,
                    row_type,
                    position,
                    before: Some(before),
                    after: Some(after),
                };
                self.push_row_to_buf(row_data).await.unwrap();
            }
        }
    }
}

impl MongoCdcExtractor {
    async fn push_row_to_buf(&mut self, row_data: RowData) -> Result<(), Error> {
        if self.filter.filter_event(
            &row_data.schema,
            &row_data.tb,
            &row_data.row_type.to_string(),
        ) {
            return Ok(());
        }
        BaseExtractor::push_row(self.buffer.as_ref(), row_data).await
    }
}
