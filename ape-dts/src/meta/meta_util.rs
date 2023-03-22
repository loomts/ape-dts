use std::collections::HashMap;

use crate::error::Error;

pub struct MetaUtil {}

impl MetaUtil {
    pub fn parse_rdb_cols(
        key_map: &HashMap<String, Vec<String>>,
        cols: &Vec<String>,
    ) -> Result<(Option<String>, String, Vec<String>), Error> {
        let mut id_cols = Vec::new();
        if let Some(cols) = key_map.get("primary") {
            // use primary key
            id_cols = cols.clone();
        } else if !key_map.is_empty() {
            // use the unique key with least cols
            for key_cols in key_map.values() {
                if id_cols.is_empty() || id_cols.len() > key_cols.len() {
                    id_cols = key_cols.clone();
                }
            }
        }

        let order_col = if id_cols.len() == 1 {
            Some(id_cols.get(0).unwrap().clone())
        } else {
            None
        };

        if id_cols.is_empty() {
            id_cols = cols.clone();
        }

        let partition_col = id_cols[0].clone();
        Ok((order_col, partition_col, id_cols))
    }
}
