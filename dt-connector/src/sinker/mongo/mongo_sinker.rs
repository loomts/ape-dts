use async_trait::async_trait;
use mongodb::{
    bson::{doc, Document},
    options::UpdateOptions,
    Client, Collection,
};

use dt_common::{constants::MongoConstants, error::Error, log_error};

use dt_meta::{col_value::ColValue, ddl_data::DdlData, row_data::RowData, row_type::RowType};

use crate::{call_batch_fn, sinker::rdb_router::RdbRouter, Sinker};

#[derive(Clone)]
pub struct MongoSinker {
    pub router: RdbRouter,
    pub batch_size: usize,
    pub mongo_client: Client,
}

#[async_trait]
impl Sinker for MongoSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> Result<(), Error> {
        if data.len() == 0 {
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

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn sink_ddl(&mut self, _data: Vec<DdlData>, _batch: bool) -> Result<(), Error> {
        Ok(())
    }
}

impl MongoSinker {
    async fn serial_sink(&mut self, mut data: Vec<RowData>) -> Result<(), Error> {
        for row_data in data.iter_mut() {
            let (db, tb) = self.router.get_route(&row_data.schema, &row_data.tb);
            let collection = self.mongo_client.database(&db).collection::<Document>(&tb);

            match row_data.row_type {
                RowType::Insert => {
                    let after = row_data.after.as_mut().unwrap();
                    if let Some(ColValue::MongoDoc(doc)) = after.remove(MongoConstants::DOC) {
                        self.upsert(&collection, &doc, &doc).await.unwrap();
                    }
                }

                RowType::Delete => {
                    let before = row_data.before.as_mut().unwrap();
                    if let Some(ColValue::MongoDoc(doc)) = before.remove(MongoConstants::DOC) {
                        collection.delete_one(doc, None).await.unwrap();
                    }
                }

                RowType::Update => {
                    let before = row_data.before.as_mut().unwrap();
                    let after = row_data.after.as_mut().unwrap();

                    let query_doc =
                        if let Some(ColValue::MongoDoc(doc)) = before.remove(MongoConstants::DOC) {
                            Some(doc)
                        } else {
                            None
                        };

                    let update_doc =
                        if let Some(ColValue::MongoDoc(doc)) = after.remove(MongoConstants::DOC) {
                            Some(doc)
                        } else {
                            None
                        };

                    if query_doc.is_some() && query_doc.is_some() {
                        self.upsert(&collection, &query_doc.unwrap(), &update_doc.unwrap())
                            .await
                            .unwrap();
                    }
                }
            }
        }
        Ok(())
    }

    async fn batch_delete(
        &mut self,
        data: &mut Vec<RowData>,
        start_index: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let (db, tb) = self.router.get_route(&data[0].schema, &data[0].tb);
        let collection = self.mongo_client.database(&db).collection::<Document>(&tb);

        let mut ids = Vec::new();
        for i in start_index..start_index + batch_size {
            let row_data = &data[i];
            let before = row_data.before.as_ref().unwrap();
            if let Some(ColValue::MongoDoc(doc)) = before.get(MongoConstants::DOC) {
                ids.push(doc.get_object_id(MongoConstants::ID).unwrap());
            }
        }

        let query = doc! {
            MongoConstants::ID: {
                "$in": ids
            }
        };
        collection.delete_many(query, None).await.unwrap();
        Ok(())
    }

    async fn batch_insert(
        &mut self,
        data: &mut Vec<RowData>,
        start_index: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let db = &data[0].schema;
        let tb = &data[0].tb;
        let collection = self.mongo_client.database(db).collection::<Document>(tb);

        let mut docs = Vec::new();
        for i in start_index..start_index + batch_size {
            let row_data = &data[i];
            let after = row_data.after.as_ref().unwrap();
            // TODO, support mysql / pg -> mongo
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
        Ok(())
    }

    async fn upsert(
        &mut self,
        collection: &Collection<Document>,
        query_doc: &Document,
        update_doc: &Document,
    ) -> Result<(), Error> {
        let query =
            doc! {MongoConstants::ID : query_doc.get_object_id(MongoConstants::ID).unwrap()};
        let update = doc! {MongoConstants::SET: update_doc};
        let options = UpdateOptions::builder().upsert(true).build();
        collection
            .update_one(query, update, Some(options))
            .await
            .unwrap();
        Ok(())
    }
}
