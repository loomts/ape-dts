use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;

use crate::{
    common::constants::Mongo,
    error::Error,
    meta::{
        col_value::ColValue, ddl_data::DdlData, dt_data::DtData, row_data::RowData,
        row_type::RowType,
    },
    traits::{Parallelizer, Sinker},
};

use super::base_parallelizer::BaseParallelizer;

pub struct MongoParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub parallel_size: usize,
}

#[async_trait]
impl Parallelizer for MongoParallelizer {
    fn get_name(&self) -> String {
        "MongoParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &ConcurrentQueue<DtData>) -> Result<Vec<DtData>, Error> {
        self.base_parallelizer.drain(buffer)
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    ) -> Result<(), Error> {
        let mut sub_datas = Self::partition_dml_by_tb(data)?;
        let mut inserts = Vec::new();
        let mut deletes = Vec::new();
        while !sub_datas.is_empty() {
            let sub_data = sub_datas.remove(0);
            let (sub_inserts, sub_deletes) = Self::merge_dml(sub_data)?;
            deletes.push(sub_deletes);
            inserts.push(sub_inserts);
        }

        self.base_parallelizer
            .sink_dml(deletes, sinkers, self.parallel_size, true)
            .await
            .unwrap();
        self.base_parallelizer
            .sink_dml(inserts, sinkers, self.parallel_size, true)
            .await
            .unwrap();
        Ok(())
    }

    async fn sink_ddl(
        &mut self,
        _data: Vec<DdlData>,
        _sinkers: &Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
    ) -> Result<(), Error> {
        Ok(())
    }
}

impl MongoParallelizer {
    /// partition dml vec into sub vecs by full table name
    pub fn partition_dml_by_tb(data: Vec<RowData>) -> Result<Vec<Vec<RowData>>, Error> {
        let mut sub_data_map: HashMap<String, Vec<RowData>> = HashMap::new();
        for row_data in data {
            let full_tb = format!("{}.{}", row_data.schema, row_data.tb);
            if let Some(sub_data) = sub_data_map.get_mut(&full_tb) {
                sub_data.push(row_data);
            } else {
                sub_data_map.insert(full_tb, vec![row_data]);
            }
        }

        let sub_datas = sub_data_map.into_iter().map(|(_, v)| v).collect();
        Ok(sub_datas)
    }

    /// partition dmls of the same table into insert vec and delete vec
    pub fn merge_dml(mut data: Vec<RowData>) -> Result<(Vec<RowData>, Vec<RowData>), Error> {
        let mut insert_map = HashMap::new();
        let mut delete_map = HashMap::new();

        while !data.is_empty() {
            let row_data = data.remove(0);
            match row_data.row_type {
                RowType::Insert => {
                    let after = row_data.after.as_ref().unwrap();
                    if let Some(ColValue::MongoDoc(doc)) = after.get(Mongo::DOC) {
                        insert_map.insert(doc.get_object_id(Mongo::ID).unwrap(), row_data);
                    }
                }

                RowType::Delete => {
                    let before = row_data.before.as_ref().unwrap();
                    if let Some(ColValue::MongoDoc(doc)) = before.get(Mongo::DOC) {
                        let id = doc.get_object_id(Mongo::ID).unwrap();
                        insert_map.remove(&id);
                        delete_map.insert(id, row_data);
                    }
                }

                RowType::Update => {
                    let before = row_data.before.unwrap();
                    let after: HashMap<String, ColValue> = row_data.after.unwrap();
                    if let Some(ColValue::MongoDoc(doc)) = before.get(Mongo::DOC) {
                        let id = doc.get_object_id(Mongo::ID).unwrap();
                        let delete_row = RowData {
                            row_type: RowType::Delete,
                            schema: row_data.schema.clone(),
                            tb: row_data.tb.clone(),
                            before: Some(before),
                            after: Option::None,
                            position: row_data.position.clone(),
                        };
                        delete_map.insert(id, delete_row);
                    }
                    if let Some(ColValue::MongoDoc(doc)) = after.get(Mongo::DOC) {
                        let id = doc.get_object_id(Mongo::ID).unwrap();
                        let insert_row = RowData {
                            row_type: RowType::Insert,
                            schema: row_data.schema,
                            tb: row_data.tb,
                            before: Option::None,
                            after: Some(after),
                            position: row_data.position,
                        };
                        insert_map.insert(id, insert_row);
                    }
                }
            }
        }

        let inserts = insert_map.drain().map(|i| i.1).collect::<Vec<_>>();
        let deletes = delete_map.drain().map(|i| i.1).collect::<Vec<_>>();
        Ok((inserts, deletes))
    }
}
