use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use async_trait::async_trait;
use mongodb::{
    bson::{doc, Document},
    options::UpdateOptions,
    Client, Collection,
};

use dt_common::{error::Error, log_error, monitor::monitor::Monitor};

use dt_meta::{
    col_value::ColValue, mongo::mongo_constant::MongoConstants, row_data::RowData,
    row_type::RowType,
};

use crate::{call_batch_fn, rdb_router::RdbRouter, sinker::base_sinker::BaseSinker, Sinker};

#[derive(Clone)]
pub struct MongoSinker {
    pub router: RdbRouter,
    pub batch_size: usize,
    pub mongo_client: Client,
    pub monitor: Arc<Mutex<Monitor>>,
}

#[async_trait]
impl Sinker for MongoSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        if !batch {
            self.serial_sink(data).await.unwrap();
        } else {
            match data[0].row_type {
                RowType::Insert => {
                    call_batch_fn!(self, data, Self::batch_insert);
                }
                RowType::Delete => {
                    call_batch_fn!(self, data, Self::batch_delete);
                }
                _ => self.serial_sink(data).await.unwrap(),
            }
        }
        Ok(())
    }
}

impl MongoSinker {
    async fn serial_sink(&mut self, mut data: Vec<RowData>) -> Result<(), Error> {
        let start_time = Instant::now();
        let mut data_size = 0;

        for row_data in data.iter_mut() {
            data_size += row_data.data_size;

            let collection = self
                .mongo_client
                .database(&row_data.schema)
                .collection::<Document>(&row_data.tb);

            match row_data.row_type {
                RowType::Insert => {
                    let after = row_data.after.as_mut().unwrap();
                    if let Some(ColValue::MongoDoc(doc)) = after.remove(MongoConstants::DOC) {
                        let query_doc =
                            doc! {MongoConstants::ID: doc.get(MongoConstants::ID).unwrap()};
                        self.upsert(&collection, query_doc, doc).await.unwrap();
                    }
                }

                RowType::Delete => {
                    let before = row_data.before.as_mut().unwrap();
                    if let Some(ColValue::MongoDoc(doc)) = before.remove(MongoConstants::DOC) {
                        let query_doc =
                            doc! {MongoConstants::ID: doc.get(MongoConstants::ID).unwrap()};
                        collection.delete_one(query_doc, None).await.unwrap();
                    }
                }

                RowType::Update => {
                    let before = row_data.before.as_mut().unwrap();
                    let after = row_data.after.as_mut().unwrap();

                    let query_doc =
                        if let Some(ColValue::MongoDoc(doc)) = before.remove(MongoConstants::DOC) {
                            Some(doc! {MongoConstants::ID: doc.get(MongoConstants::ID).unwrap()})
                        } else {
                            None
                        };

                    let update_doc =
                        if let Some(ColValue::MongoDoc(doc)) = after.remove(MongoConstants::DOC) {
                            Some(doc)
                        } else if let Some(ColValue::MongoDoc(doc)) =
                            after.remove(MongoConstants::DIFF_DOC)
                        {
                            // for Update row_data from oplog (NOT change stream), after contains diff_doc instead of doc
                            Some(doc)
                        } else {
                            None
                        };

                    if query_doc.is_some() && update_doc.is_some() {
                        self.upsert(&collection, query_doc.unwrap(), update_doc.unwrap())
                            .await
                            .unwrap();
                    }
                }
            }
        }

        BaseSinker::update_serial_monitor(&mut self.monitor, data.len(), data_size, start_time)
            .await
    }

    async fn batch_delete(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let start_time = Instant::now();
        let mut data_size = 0;

        let collection = self
            .mongo_client
            .database(&data[0].schema)
            .collection::<Document>(&data[0].tb);

        let mut ids = Vec::new();
        for rd in data.iter().skip(start_index).take(batch_size) {
            data_size += rd.data_size;

            let before = rd.before.as_ref().unwrap();
            if let Some(ColValue::MongoDoc(doc)) = before.get(MongoConstants::DOC) {
                ids.push(doc.get(MongoConstants::ID).unwrap());
            }
        }

        let query = doc! {
            MongoConstants::ID: {
                "$in": ids
            }
        };
        collection.delete_many(query, None).await.unwrap();

        BaseSinker::update_batch_monitor(&mut self.monitor, batch_size, data_size, start_time).await
    }

    async fn batch_insert(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let start_time = Instant::now();
        let mut data_size = 0;

        let db = &data[0].schema;
        let tb = &data[0].tb;
        let collection = self.mongo_client.database(db).collection::<Document>(tb);

        let mut docs = Vec::new();
        for rd in data.iter().skip(start_index).take(batch_size) {
            data_size += rd.data_size;

            let after = rd.after.as_ref().unwrap();
            if let Some(ColValue::MongoDoc(doc)) = after.get(MongoConstants::DOC) {
                docs.push(doc);
            }
        }

        if let Err(error) = collection.insert_many(docs, None).await {
            log_error!(
                "batch insert failed, will insert one by one, schema: {}, tb: {}, error: {}",
                db,
                tb,
                error.to_string()
            );
            let sub_data = &data[start_index..start_index + batch_size];
            self.serial_sink(sub_data.to_vec()).await.unwrap();
        }

        BaseSinker::update_batch_monitor(&mut self.monitor, batch_size, data_size, start_time).await
    }

    async fn upsert(
        &mut self,
        collection: &Collection<Document>,
        query_doc: Document,
        update_doc: Document,
    ) -> Result<(), Error> {
        let update = doc! {MongoConstants::SET: update_doc};
        let options = UpdateOptions::builder().upsert(true).build();
        collection
            .update_one(query_doc, update, Some(options))
            .await
            .unwrap();
        Ok(())
    }
}
