use std::collections::HashMap;

use crate::{
    error::Error,
    meta::{rdb_meta_manager::RdbMetaManager, row_data::RowData, row_type::RowType},
};

pub struct RdbMerger {
    pub meta_manager: RdbMetaManager,
}

impl RdbMerger {
    pub async fn merge(
        &mut self,
        data: Vec<RowData>,
    ) -> Result<HashMap<String, TbMergedData>, Error> {
        let mut sub_datas = HashMap::<String, TbMergedData>::new();
        for row_data in data {
            let full_tb = format!("{}.{}", row_data.schema, row_data.tb);
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

        let tb_meta = self
            .meta_manager
            .get_tb_meta(&row_data.schema, &row_data.tb)
            .await?;
        match row_data.row_type {
            RowType::Delete => {
                if self.check_collision(&merged.insert_rows, &tb_meta.id_cols, &row_data, hash_code)
                    || self.check_collision(
                        &merged.delete_rows,
                        &tb_meta.id_cols,
                        &row_data,
                        hash_code,
                    )
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

                if self.check_collision(
                    &merged.insert_rows,
                    &tb_meta.id_cols,
                    &insert,
                    insert_hash_code,
                ) || self.check_collision(
                    &merged.delete_rows,
                    &tb_meta.id_cols,
                    &delete,
                    hash_code,
                ) {
                    let row_data = RowData {
                        row_type: RowType::Update,
                        schema: delete.schema,
                        tb: delete.tb,
                        before: delete.before,
                        after: insert.after,
                        position: delete.position,
                    };
                    merged.unmerged_rows.push(row_data);
                    return Ok(());
                }
                merged.delete_rows.insert(hash_code, delete);
                merged.insert_rows.insert(insert_hash_code, insert);
            }

            RowType::Insert => {
                if self.check_collision(&merged.insert_rows, &tb_meta.id_cols, &row_data, hash_code)
                {
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
        id_cols: &Vec<String>,
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

            for col in id_cols.iter() {
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
            schema: row_data.schema.clone(),
            tb: row_data.tb.clone(),
            before: row_data.before,
            after: Option::None,
            position: row_data.position.clone(),
        };

        let insert_row = RowData {
            row_type: RowType::Insert,
            schema: row_data.schema,
            tb: row_data.tb,
            before: Option::None,
            after: row_data.after,
            position: row_data.position,
        };

        Ok((delete_row, insert_row))
    }

    async fn get_hash_code(&mut self, row_data: &RowData) -> Result<u128, Error> {
        let tb_meta = self
            .meta_manager
            .get_tb_meta(&row_data.schema, &row_data.tb)
            .await?;
        if tb_meta.key_map.is_empty() {
            return Ok(0);
        }
        Ok(row_data.get_hash_code(&tb_meta))
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
