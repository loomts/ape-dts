use std::{collections::HashMap, sync::atomic::AtomicBool};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::{
    constants::MongoConstants,
    error::Error,
    log_info,
    meta::{col_value::ColValue, dt_data::DtData, row_data::RowData, row_type::RowType},
};
use mongodb::{
    bson::doc,
    change_stream::event::{OperationType, ResumeToken},
    options::{ChangeStreamOptions, FullDocumentBeforeChangeType, FullDocumentType},
    Client,
};
use serde_json::json;

use crate::{
    extractor::{base_extractor::BaseExtractor, rdb_filter::RdbFilter},
    Extractor,
};

pub struct MongoCdcExtractor<'a> {
    pub buffer: &'a ConcurrentQueue<DtData>,
    pub filter: RdbFilter,
    pub shut_down: &'a AtomicBool,
    pub resume_token: String,
    pub mongo_client: Client,
}

#[async_trait]
impl Extractor for MongoCdcExtractor<'_> {
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

impl MongoCdcExtractor<'_> {
    async fn extract_internal(&mut self) -> Result<(), Error> {
        let start_after = if self.resume_token.is_empty() {
            None
        } else {
            let token: ResumeToken = serde_json::from_str(&self.resume_token).unwrap();
            Some(token)
        };
        let stream_options = ChangeStreamOptions::builder()
            .start_after(start_after)
            .full_document(Some(FullDocumentType::UpdateLookup))
            .full_document_before_change(Some(FullDocumentBeforeChangeType::WhenAvailable))
            .build();

        let mut change_stream = self.mongo_client.watch(None, stream_options).await.unwrap();
        loop {
            let result = change_stream.next_if_any().await.unwrap();
            match result {
                Some(doc) => {
                    let resume_token = doc.id;
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
                            let id = doc
                                .full_document
                                .as_ref()
                                .unwrap()
                                .get_object_id(MongoConstants::ID)
                                .unwrap();
                            let before_doc = doc! {MongoConstants::ID: id};
                            let after_doc = doc.full_document.unwrap();
                            before.insert(
                                MongoConstants::DOC.to_string(),
                                ColValue::MongoDoc(before_doc),
                            );
                            after.insert(
                                MongoConstants::DOC.to_string(),
                                ColValue::MongoDoc(after_doc),
                            );
                        }

                        _ => {}
                    }

                    let row_data = RowData {
                        schema: db,
                        tb,
                        row_type,
                        position: json!(resume_token).to_string(),
                        before: Some(before),
                        after: Some(after),
                    };
                    self.push_row_to_buf(row_data).await.unwrap();
                }

                None => {}
            }
        }
    }
}

impl MongoCdcExtractor<'_> {
    async fn push_row_to_buf(&mut self, row_data: RowData) -> Result<(), Error> {
        if self
            .filter
            .filter_event(&row_data.schema, &row_data.tb, &row_data.row_type)
        {
            return Ok(());
        }
        BaseExtractor::push_row(&self.buffer, row_data).await
    }
}
