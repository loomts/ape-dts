use std::collections::HashMap;

use crate::{
    error::Error,
    meta::{
        mysql::mysql_meta_manager::MysqlMetaManager, pg::pg_meta_manager::PgMetaManager,
        row_data::RowData, row_type::RowType,
    },
};

use super::rdb_partitioner::RdbPartitioner;

pub struct RdbMerger {
    rdb_partitioner: RdbPartitioner,
}

impl RdbMerger {
    pub fn new_for_mysql(meta_manager: MysqlMetaManager) -> RdbMerger {
        Self {
            rdb_partitioner: RdbPartitioner::new_for_mysql(meta_manager),
        }
    }

    pub fn new_for_pg(meta_manager: PgMetaManager) -> RdbMerger {
        Self {
            rdb_partitioner: RdbPartitioner::new_for_pg(meta_manager),
        }
    }

    pub async fn merge(
        &mut self,
        data: Vec<RowData>,
    ) -> Result<HashMap<String, TbMergedData>, Error> {
        let mut sub_datas = HashMap::<String, TbMergedData>::new();
        for row_data in data {
            let full_tb = format!("{}.{}", row_data.db, row_data.tb);
            if let Some(merged) = sub_datas.get_mut(&full_tb) {
                self.merge_row_data(merged, row_data).await?;
            } else {
                let mut merged = TbMergedData::new();
                self.merge_row_data(&mut merged, row_data).await?;
                sub_datas.insert(full_tb, merged);
            }
        }
        Ok(sub_datas)
    }

    async fn merge_row_data(
        &mut self,
        merged: &mut TbMergedData,
        row_data: RowData,
    ) -> Result<(), Error> {
        // if the table already has some rows unmerged, then following rows also need to be unmerged.
        // all unmerged rows will be sinked serially
        if !merged.unmerged_rows.is_empty() {
            merged.unmerged_rows.push(row_data);
            return Ok(());
        }

        // case 1: table has no primary/unique key
        // case 2: any key col value is NULL
        let hash_code = self.get_hash_code(&row_data).await?;
        if hash_code == 0 {
            merged.unmerged_rows.push(row_data);
            return Ok(());
        }

        let (_, _, where_cols) = self
            .rdb_partitioner
            .get_tb_meta_info(&row_data.db, &row_data.tb)
            .await?;
        match row_data.row_type {
            RowType::Delete => {
                if self.check_collision(&merged.insert_rows, &where_cols, &row_data, hash_code)
                    || self.check_collision(&merged.delete_rows, &where_cols, &row_data, hash_code)
                {
                    merged.unmerged_rows.push(row_data);
                    return Ok(());
                }
                merged.insert_rows.remove(&hash_code);
                merged.delete_rows.insert(hash_code, row_data);
            }

            RowType::Update => {
                let (delete, insert) = self.split_update_row_data(row_data).await?;
                let insert_hash_code = self.get_hash_code(&insert).await?;

                if self.check_collision(&merged.insert_rows, &where_cols, &insert, insert_hash_code)
                    || self.check_collision(&merged.delete_rows, &where_cols, &delete, hash_code)
                {
                    let row_data = RowData {
                        row_type: RowType::Update,
                        db: delete.db,
                        tb: delete.tb,
                        before: delete.before,
                        after: insert.after,
                        current_position: delete.current_position,
                        checkpoint_position: delete.checkpoint_position,
                    };
                    merged.unmerged_rows.push(row_data);
                    return Ok(());
                }
                merged.delete_rows.insert(hash_code, delete);
                merged.insert_rows.insert(insert_hash_code, insert);
            }

            RowType::Insert => {
                if self.check_collision(&merged.insert_rows, &where_cols, &row_data, hash_code) {
                    merged.unmerged_rows.push(row_data);
                    return Ok(());
                }
                merged.insert_rows.insert(hash_code, row_data);
            }
        }
        Ok(())
    }

    fn check_collision(
        &mut self,
        buffer: &HashMap<u128, RowData>,
        where_cols: &Vec<String>,
        row_data: &RowData,
        hash_code: u128,
    ) -> bool {
        if let Some(exist) = buffer.get(&hash_code) {
            let col_values = match row_data.row_type {
                RowType::Insert => row_data.after.as_ref().unwrap(),
                _ => row_data.before.as_ref().unwrap(),
            };

            let exist_col_values = match exist.row_type {
                RowType::Insert => exist.after.as_ref().unwrap(),
                _ => exist.before.as_ref().unwrap(),
            };

            for col in where_cols.iter() {
                if col_values.get(col) != exist_col_values.get(col) {
                    return true;
                }
            }
        }
        false
    }

    async fn split_update_row_data(
        &mut self,
        row_data: RowData,
    ) -> Result<(RowData, RowData), Error> {
        let delete_row = RowData {
            row_type: RowType::Delete,
            db: row_data.db.clone(),
            tb: row_data.tb.clone(),
            before: row_data.before,
            after: Option::None,
            current_position: row_data.current_position.clone(),
            checkpoint_position: row_data.checkpoint_position.clone(),
        };

        let insert_row = RowData {
            row_type: RowType::Insert,
            db: row_data.db,
            tb: row_data.tb,
            before: Option::None,
            after: row_data.after,
            current_position: row_data.current_position,
            checkpoint_position: row_data.checkpoint_position,
        };

        Ok((delete_row, insert_row))
    }

    async fn get_hash_code(&mut self, row_data: &RowData) -> Result<u128, Error> {
        let col_values = match row_data.row_type {
            RowType::Insert => row_data.after.as_ref().unwrap(),
            _ => row_data.before.as_ref().unwrap(),
        };

        let (_, key_map, where_cols) = self
            .rdb_partitioner
            .get_tb_meta_info(&row_data.db, &row_data.tb)
            .await?;
        if key_map.is_empty() {
            return Ok(0);
        }

        // refer to: https://docs.oracle.com/javase/6/docs/api/java/util/List.html#hashCode%28%29
        let mut hash_code = 1u128;
        let mut key_col_hash_codes = Vec::new();
        for col in where_cols {
            let col_hash_code = col_values.get(&col).unwrap().hash_code();
            // col_hash_code is 0 if col_value is ColValue::None,
            // consider fowlling case,
            // create table a(id int, value int, unique key(id, value));
            // insert into a values(1, NULL);
            // delete from a where (id, value) in ((1, NULL));  // this won't work
            // delete from a where id=1 and value is NULL;  // this works
            // so here return 0 to stop merging to avoid batch deleting
            if col_hash_code == 0 {
                return Ok(0);
            }
            hash_code = 31 * hash_code + col_hash_code as u128;
            key_col_hash_codes.push(col_hash_code);
        }
        Ok(hash_code)
    }
}

pub struct TbMergedData {
    // HashMap<row_key_hash_code, RowData>
    delete_rows: HashMap<u128, RowData>,
    insert_rows: HashMap<u128, RowData>,
    unmerged_rows: Vec<RowData>,
}

impl TbMergedData {
    pub fn new() -> Self {
        Self {
            delete_rows: HashMap::new(),
            insert_rows: HashMap::new(),
            unmerged_rows: Vec::new(),
        }
    }

    pub fn get_delete_rows(&mut self) -> Vec<RowData> {
        self.delete_rows.drain().map(|i| i.1).collect::<Vec<_>>()
    }

    pub fn get_insert_rows(&mut self) -> Vec<RowData> {
        self.insert_rows.drain().map(|i| i.1).collect::<Vec<_>>()
    }

    pub fn get_unmerged_rows(&mut self) -> Vec<RowData> {
        self.unmerged_rows.as_slice().to_vec()
    }
}
