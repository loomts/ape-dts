use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::error::Error;
use dt_connector::Sinker;
use dt_meta::{
    col_value::ColValue,
    ddl_data::DdlData,
    dt_data::DtData,
    mongo::{mongo_constant::MongoConstants, mongo_key::MongoKey},
    row_data::RowData,
    row_type::RowType,
};

use crate::Parallelizer;

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
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> Result<(), Error> {
        let mut sub_datas = Self::partition_dml_by_tb(data)?;
        let mut inserts = Vec::new();
        let mut deletes = Vec::new();
        let mut unmerged = Vec::new();
        while !sub_datas.is_empty() {
            let sub_data = sub_datas.remove(0);
            let (sub_inserts, sub_deletes, unmerged_rows) = Self::merge_dml(sub_data)?;
            deletes.push(sub_deletes);
            inserts.push(sub_inserts);
            unmerged.push(unmerged_rows);
        }

        self.base_parallelizer
            .sink_dml(deletes, sinkers, self.parallel_size, true)
            .await
            .unwrap();
        self.base_parallelizer
            .sink_dml(inserts, sinkers, self.parallel_size, true)
            .await
            .unwrap();
        self.base_parallelizer
            .sink_dml(unmerged, sinkers, 1, false)
            .await
            .unwrap();
        Ok(())
    }

    async fn sink_ddl(
        &mut self,
        _data: Vec<DdlData>,
        _sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
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

        Ok(sub_data_map.into_values().collect())
    }

    /// partition dmls of the same table into insert vec and delete vec
    pub fn merge_dml(
        mut data: Vec<RowData>,
    ) -> Result<(Vec<RowData>, Vec<RowData>, Vec<RowData>), Error> {
        let mut insert_map = HashMap::new();
        let mut delete_map = HashMap::new();

        while !data.is_empty() {
            let hash_key = Self::get_hash_key(&data[0]);
            if hash_key.is_none() {
                break;
            }

            let id = hash_key.unwrap();
            let row_data = data.remove(0);
            match row_data.row_type {
                RowType::Insert => {
                    insert_map.insert(id, row_data);
                }

                RowType::Delete => {
                    insert_map.remove(&id);
                    delete_map.insert(id, row_data);
                }

                RowType::Update => {
                    let before = row_data.before.unwrap();
                    let after: HashMap<String, ColValue> = row_data.after.unwrap();
                    let delete_row = RowData {
                        row_type: RowType::Delete,
                        schema: row_data.schema.clone(),
                        tb: row_data.tb.clone(),
                        before: Some(before),
                        after: Option::None,
                        position: row_data.position.clone(),
                    };
                    delete_map.insert(id.clone(), delete_row);

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

        let inserts = insert_map.drain().map(|i| i.1).collect::<Vec<_>>();
        let deletes = delete_map.drain().map(|i| i.1).collect::<Vec<_>>();
        Ok((inserts, deletes, data))
    }

    fn get_hash_key(row_data: &RowData) -> Option<MongoKey> {
        match row_data.row_type {
            RowType::Insert => {
                let after = row_data.after.as_ref().unwrap();
                if let Some(ColValue::MongoDoc(doc)) = after.get(MongoConstants::DOC) {
                    return MongoKey::from_doc(doc);
                }
            }

            RowType::Delete => {
                let before = row_data.before.as_ref().unwrap();
                if let Some(ColValue::MongoDoc(doc)) = before.get(MongoConstants::DOC) {
                    return MongoKey::from_doc(doc);
                }
            }

            RowType::Update => {
                let before = row_data.before.as_ref().unwrap();
                let after = row_data.after.as_ref().unwrap();
                // for Update row_data from oplog (NOT change stream), after contains diff_doc instead of doc,
                // in which case we can NOT transfer Update into Delete + Insert
                if after.get(MongoConstants::DOC).is_none() {
                    return None;
                } else if let Some(ColValue::MongoDoc(doc)) = before.get(MongoConstants::DOC) {
                    return MongoKey::from_doc(doc);
                }
            }
        }
        return None;
    }
}
