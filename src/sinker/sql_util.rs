use std::collections::HashMap;

use crate::{
    error::Error,
    meta::{col_value::ColValue, row_data::RowData, tb_meta::TbMeta},
};

pub struct SqlUtil {}

impl SqlUtil {
    pub fn get_batch_insert_sql(tb_meta: &TbMeta, batch_size: usize) -> Result<String, Error> {
        let mut col_values = Vec::new();
        for _ in tb_meta.cols.iter() {
            col_values.push("?");
        }
        let col_values_str = format!("({})", col_values.join(","));

        let mut row_values = Vec::new();
        for _ in 0..batch_size {
            row_values.push(col_values_str.as_str());
        }

        let sql = format!(
            "REPLACE INTO {}.{}({}) VALUES{}",
            tb_meta.db,
            tb_meta.tb,
            tb_meta.cols.join(","),
            row_values.join(",")
        );
        Ok(sql)
    }

    pub fn get_insert_sql<'a>(
        row_data: &'a RowData,
        tb_meta: &TbMeta,
    ) -> Result<(String, Vec<Option<&'a ColValue>>), Error> {
        let mut col_values = Vec::new();
        for _ in tb_meta.cols.iter() {
            col_values.push("?");
        }

        let sql = format!(
            "REPLACE INTO {}.{}({}) VALUES({})",
            tb_meta.db,
            tb_meta.tb,
            tb_meta.cols.join(","),
            col_values.join(",")
        );

        let mut binds = Vec::new();
        let after = row_data.after.as_ref().unwrap();
        for col_name in tb_meta.cols.iter() {
            binds.push(after.get(col_name));
        }
        Ok((sql, binds))
    }

    pub fn get_delete_sql<'a>(
        row_data: &'a RowData,
        tb_meta: &TbMeta,
    ) -> Result<(String, Vec<Option<&'a ColValue>>), Error> {
        let before = row_data.before.as_ref().unwrap();
        let (where_sql, not_null_cols) = Self::get_where_info(&tb_meta, &before)?;
        let sql = format!(
            "DELETE FROM {}.{} WHERE {} LIMIT 1",
            tb_meta.db, tb_meta.tb, where_sql,
        );

        let mut binds = Vec::new();
        for col_name in not_null_cols.iter() {
            binds.push(before.get(col_name));
        }
        Ok((sql, binds))
    }

    pub fn get_update_sql<'a>(
        row_data: &'a RowData,
        tb_meta: &TbMeta,
    ) -> Result<(String, Vec<Option<&'a ColValue>>), Error> {
        let before = row_data.before.as_ref().unwrap();
        let after = row_data.after.as_ref().unwrap();

        let mut set_cols = Vec::new();
        let mut set_pairs = Vec::new();
        for (col_name, _) in after.iter() {
            set_cols.push(col_name.clone());
            set_pairs.push(format!("{}=?", col_name));
        }

        let (where_sql, not_null_cols) = Self::get_where_info(&tb_meta, &before)?;
        let sql = format!(
            "UPDATE {}.{} SET {} WHERE {} LIMIT 1",
            tb_meta.db,
            tb_meta.tb,
            set_pairs.join(","),
            where_sql,
        );

        let mut binds = Vec::new();
        for col_name in set_cols.iter() {
            binds.push(after.get(col_name));
        }
        for col_name in not_null_cols.iter() {
            binds.push(before.get(col_name));
        }
        Ok((sql, binds))
    }

    fn get_where_info(
        tb_meta: &TbMeta,
        col_value_map: &HashMap<String, ColValue>,
    ) -> Result<(String, Vec<String>), Error> {
        let mut where_sql = "".to_string();
        let mut not_null_cols = Vec::new();

        for col_name in tb_meta.where_cols.iter() {
            if !where_sql.is_empty() {
                where_sql += " AND";
            }

            let col_value = col_value_map.get(col_name);
            if let Some(value) = col_value {
                if *value == ColValue::None {
                    where_sql = format!("{} {} IS NULL", where_sql, col_name);
                } else {
                    where_sql = format!("{} {} = ?", where_sql, col_name);
                    not_null_cols.push(col_name.clone());
                }
            } else {
                where_sql = format!("{} {} IS NULL", where_sql, col_name);
            }
        }

        Ok((where_sql, not_null_cols))
    }
}
