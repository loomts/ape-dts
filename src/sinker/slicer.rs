use crate::{
    error::Error,
    meta::{db_meta_manager::DbMetaManager, row_data::RowData, row_type::RowType},
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
