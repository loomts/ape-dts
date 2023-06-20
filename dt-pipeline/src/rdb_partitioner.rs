use dt_common::{error::Error, log_debug};
use dt_meta::{rdb_meta_manager::RdbMetaManager, row_data::RowData, row_type::RowType};

pub struct RdbPartitioner {
    pub meta_manager: RdbMetaManager,
}

impl RdbPartitioner {
    pub async fn partition(
        &mut self,
        data: Vec<RowData>,
        partition_count: usize,
    ) -> Result<Vec<Vec<RowData>>, Error> {
        let mut sub_datas = Vec::new();
        if partition_count <= 1 {
            sub_datas.push(data);
            return Ok(sub_datas);
        }

        for _ in 0..partition_count {
            sub_datas.push(Vec::new());
        }

        for row_data in data {
            let partition = self.get_partition_index(&row_data, partition_count).await?;
            sub_datas[partition].push(row_data);
        }

        Ok(sub_datas)
    }

    pub async fn can_be_partitioned<'a>(&mut self, row_data: &'a RowData) -> Result<bool, Error> {
        if row_data.row_type != RowType::Update {
            return Ok(true);
        }

        let tb_meta = self
            .meta_manager
            .get_tb_meta(&row_data.schema, &row_data.tb)
            .await?;
        let before = row_data.before.as_ref().unwrap();
        let after = row_data.after.as_ref().unwrap();
        // if any col of pk & uk has changed, partition should not happen
        // example:
        // create table a(f1 int, f2 int, primary key(f1), unique key(f2));
        // insert a: [(1, 1) (2, 2)]
        // update a: [(1, 1) -> (1, 3), (2, 2) -> (2, 1)]
        // if partitioned by 2,
        // partition 1: [insert (1, 1), update((1, 1) -> (1, 3))]
        // partition 2: [insert (2, 2), update((2, 2) -> (2, 1))]
        // partitions will be sinked parallely, the below case may happen:
        // partition 1 sinked: [insert (1, 1)]
        // partition 2 sinked: [insert (2, 2)]
        // if partition 2 sinking: [update((2, 2) -> (2, 1))] happens before
        //    partition 1 sinking: [update((1, 1) -> (1, 3))], it would fail
        for key_cols in tb_meta.key_map.values() {
            for col in key_cols {
                let col_value_before = before.get(col);
                let col_value_after = after.get(col);
                if col_value_before != col_value_after {
                    log_debug!(
                        "{}.{}.{} changed from {:?} to {:?}",
                        &row_data.schema,
                        &row_data.tb,
                        col,
                        col_value_before.unwrap().to_option_string(),
                        col_value_after.unwrap().to_option_string()
                    );
                    return Ok(false);
                }
            }
        }
        // no need to check parition_col if key_map is not empty,
        // in which case partition_col is one of the key cols and has been checked
        if tb_meta.key_map.is_empty()
            && before.get(&tb_meta.partition_col) != after.get(&tb_meta.partition_col)
        {
            return Ok(false);
        }

        Ok(true)
    }

    async fn get_partition_index(
        &mut self,
        row_data: &RowData,
        slice_count: usize,
    ) -> Result<usize, Error> {
        if slice_count <= 1 {
            return Ok(0);
        }

        let col_values = match row_data.row_type {
            RowType::Insert => row_data.after.as_ref().unwrap(),
            _ => row_data.before.as_ref().unwrap(),
        };

        let tb_meta = self
            .meta_manager
            .get_tb_meta(&row_data.schema, &row_data.tb)
            .await?;
        if let Some(partition_col_value) = col_values.get(&tb_meta.partition_col) {
            Ok(partition_col_value.hash_code() as usize % slice_count)
        } else {
            Ok(0)
        }
    }
}
