use crate::{
    error::Error,
    meta::{
        col_value::ColValue, db_meta_manager::DbMetaManager, row_data::RowData, row_type::RowType,
    },
};

pub struct Slicer {
    pub db_meta_manager: DbMetaManager,
}

impl Slicer {
    pub async fn slice(
        &mut self,
        data: Vec<RowData>,
        slice_count: usize,
    ) -> Result<Vec<Vec<RowData>>, Error> {
        let mut sub_datas = Vec::new();
        for _ in 0..slice_count {
            sub_datas.push(Vec::new());
        }

        for row_data in data {
            let partition = self.partition(&row_data, slice_count).await?;
            sub_datas[partition].push(row_data);
        }

        Ok(sub_datas)
    }

    pub async fn check_uk_col_changed<'a>(
        &mut self,
        row_data: &'a RowData,
    ) -> Result<
        (
            bool,
            Option<String>,
            Option<&'a ColValue>,
            Option<&'a ColValue>,
        ),
        Error,
    > {
        if row_data.row_type != RowType::Update {
            return Ok((false, Option::None, Option::None, Option::None));
        }

        let tb_meta = self
            .db_meta_manager
            .get_tb_meta(&row_data.db, &row_data.tb)
            .await?;

        let before = row_data.before.as_ref().unwrap();
        let after = row_data.after.as_ref().unwrap();
        // check if any col value of pk & uk has changed
        for key_cols in tb_meta.key_map.values() {
            for col in key_cols {
                let col_value_before = before.get(col);
                let col_value_after = after.get(col);
                if col_value_before != col_value_after {
                    return Ok((
                        true,
                        Some(col.to_string()),
                        col_value_before,
                        col_value_after,
                    ));
                }
            }
        }
        Ok((false, Option::None, Option::None, Option::None))
    }

    async fn partition(&mut self, row_data: &RowData, slice_count: usize) -> Result<usize, Error> {
        if slice_count <= 1 {
            return Ok(0);
        }

        let col_values = match row_data.row_type {
            RowType::Insert => row_data.after.as_ref().unwrap(),
            _ => row_data.before.as_ref().unwrap(),
        };

        let tb_meta = self
            .db_meta_manager
            .get_tb_meta(&row_data.db, &row_data.tb)
            .await?;

        if let Some(partition_col_value) = col_values.get(&tb_meta.partition_col) {
            Ok(partition_col_value.hash_code() as usize % slice_count)
        } else {
            Ok(0)
        }
    }
}
