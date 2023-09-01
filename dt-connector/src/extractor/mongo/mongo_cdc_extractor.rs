use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
};

use async_trait::async_trait;
use chrono::Utc;
use concurrent_queue::ConcurrentQueue;
use dt_common::{
    error::Error,
    log_info,
    utils::{position_util::PositionUtil, rdb_filter::RdbFilter},
};
use dt_meta::{
    col_value::ColValue,
    dt_data::DtData,
    mongo::{mongo_cdc_source::MongoCdcSource, mongo_constant::MongoConstants},
    row_data::RowData,
    row_type::RowType,
};
use mongodb::{
    bson::{doc, Document, Timestamp},
    change_stream::event::{OperationType, ResumeToken},
    options::{ChangeStreamOptions, FullDocumentBeforeChangeType, FullDocumentType},
    Client,
};
use serde_json::json;

use crate::{extractor::base_extractor::BaseExtractor, Extractor};

const SYSTEM_DBS: [&str; 3] = ["admin", "config", "local"];

pub struct MongoCdcExtractor {
    pub buffer: Arc<ConcurrentQueue<DtData>>,
    pub filter: RdbFilter,
    pub shut_down: Arc<AtomicBool>,
    pub resume_token: String,
    pub start_timestamp: u32,
    pub source: MongoCdcSource,
    pub mongo_client: Client,
}

#[async_trait]
impl Extractor for MongoCdcExtractor {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!(
            "MongoCdcExtractor starts, resume_token: {}, start_timestamp: {}, source: {:?} ",
            self.resume_token,
            self.start_timestamp,
            self.source,
        );

        match self.source {
            MongoCdcSource::OpLog => self.extract_oplog().await,
            MongoCdcSource::ChangeStream => self.extract_change_stream().await,
        }
    }
}

impl MongoCdcExtractor {
    async fn extract_oplog(&mut self) -> Result<(), Error> {
        let start_timestamp = self.parse_start_timestamp();
        let filter = doc! {
            "ts": { "$gte": start_timestamp }
        };
        let options = mongodb::options::FindOptions::builder()
            .cursor_type(mongodb::options::CursorType::TailableAwait)
            .build();

        let oplog = self
            .mongo_client
            .database("local")
            .collection::<Document>("oplog.rs");
        let mut cursor = oplog.find(filter, options).await.unwrap();

        while cursor.advance().await.unwrap() {
            let doc: Document = cursor.deserialize_current().unwrap();
            // https://github.com/mongodb/mongo/blob/master/src/mongo/db/repl/oplog.cpp
            // op:
            //     "i" insert
            //     "u" update
            //     "d" delete
            //     "c" db cmd
            //     "n" no op
            //     "xi" insert global index key
            //     "xd" delete global index key
            let op = if let Some(op) = doc.get("op") {
                op.as_str().unwrap()
            } else {
                ""
            };

            let mut row_type = RowType::Insert;
            let mut before = HashMap::new();
            let mut after = HashMap::new();
            let o = doc.get("o");
            let o2 = doc.get("o2");
            match op {
                "i" => {
                    after.insert(
                        MongoConstants::DOC.to_string(),
                        ColValue::MongoDoc(o.unwrap().as_document().unwrap().clone()),
                    );
                }
                "u" => {
                    row_type = RowType::Update;
                    // for update op log, doc.o contains only diff instead of full doc
                    let after_doc = o.unwrap().as_document().unwrap();
                    let diff_doc = after_doc.get("diff").unwrap().as_document().unwrap();
                    let u_doc = diff_doc.get("u").unwrap().as_document().unwrap();
                    after.insert(
                        MongoConstants::DIFF_DOC.to_string(),
                        ColValue::MongoDoc(u_doc.clone()),
                    );
                    before.insert(
                        MongoConstants::DOC.to_string(),
                        ColValue::MongoDoc(o2.unwrap().as_document().unwrap().clone()),
                    );
                }
                "d" => {
                    row_type = RowType::Delete;
                    before.insert(
                        MongoConstants::DOC.to_string(),
                        ColValue::MongoDoc(o.unwrap().as_document().unwrap().clone()),
                    );
                }
                // TODO, DDL
                "c" => {}
                "xi" => {}
                "xd" => {}
                "n" => {
                    // TODO, heartbeat
                    // Document({"op": String("n"), "ns": String(""), "o": Document({"msg": String("periodic noop")}), "ts": Timestamp { time: 1693470874, increment: 1 }, "t": Int64(67), "v": Int64(2), "wall": DateTime(2023-08-31 8:34:34.19 +00:00:00)})
                    continue;
                }
                _ => {
                    continue;
                }
            }

            // get db & tb
            let ns = doc.get("ns").unwrap().as_str().unwrap();
            let tokens: Vec<&str> = ns.split(".").collect();
            let db: String = tokens[0].into();
            let tb: String = ns[db.len() + 1..].into();

            // get ts for position
            let ts = doc.get("ts").unwrap().as_timestamp().unwrap();
            let position = format!(
                "resume_token:,operation_time:{},timestamp:{}",
                ts.time,
                PositionUtil::format_timestamp_millis(ts.time as i64 * 1000)
            );

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
        Ok(())
    }

    async fn extract_change_stream(&mut self) -> Result<(), Error> {
        let (resume_token, start_timestamp) = if self.resume_token.is_empty() {
            (None, Some(self.parse_start_timestamp()))
        } else {
            let token: ResumeToken = serde_json::from_str(&self.resume_token).unwrap();
            (Some(token), None)
        };

        let stream_options = ChangeStreamOptions::builder()
            .start_at_operation_time(start_timestamp)
            .start_after(resume_token)
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
                            before.insert(
                                MongoConstants::DOC.to_string(),
                                ColValue::MongoDoc(doc.document_key.unwrap()),
                            );
                            after.insert(
                                MongoConstants::DOC.to_string(),
                                ColValue::MongoDoc(document),
                            );
                        }
                    }

                    // TODO, heartbeat and DDL
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

    async fn push_row_to_buf(&mut self, row_data: RowData) -> Result<(), Error> {
        if SYSTEM_DBS.contains(&row_data.schema.as_str())
            || self.filter.filter_event(
                &row_data.schema,
                &row_data.tb,
                &row_data.row_type.to_string(),
            )
        {
            return Ok(());
        }
        BaseExtractor::push_row(self.buffer.as_ref(), row_data).await
    }

    fn parse_start_timestamp(&mut self) -> Timestamp {
        let time = if self.start_timestamp > 0 {
            self.start_timestamp
        } else {
            Utc::now().timestamp() as u32
        };
        Timestamp { time, increment: 0 }
    }
}
