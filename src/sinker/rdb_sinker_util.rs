use std::collections::HashMap;

use log::info;

use crate::{
    error::Error,
    meta::{
        col_value::ColValue, mysql::mysql_tb_meta::MysqlTbMeta, pg::pg_tb_meta::PgTbMeta,
        row_data::RowData, row_type::RowType,
    },
};

pub struct RdbSinkerUtil {
    schema: String,
    tb: String,
    cols: Vec<String>,
    where_cols: Vec<String>,
    key_map: HashMap<String, Vec<String>>,
}

impl RdbSinkerUtil {
    pub fn new_for_mysql(tb_meta: MysqlTbMeta) -> RdbSinkerUtil {
        RdbSinkerUtil {
            schema: tb_meta.db,
            tb: tb_meta.tb,
            cols: tb_meta.cols,
            where_cols: tb_meta.where_cols,
            key_map: tb_meta.key_map,
        }
    }

    pub fn new_for_pg(tb_meta: PgTbMeta) -> RdbSinkerUtil {
        RdbSinkerUtil {
            schema: tb_meta.schema,
            tb: tb_meta.tb,
            cols: tb_meta.cols,
            where_cols: tb_meta.where_cols,
            key_map: tb_meta.key_map,
        }
    }

    pub fn get_query<'a>(
        &self,
        row_data: &'a RowData,
    ) -> Result<(String, Vec<String>, Vec<Option<&'a ColValue>>), Error> {
        let (sql, cols, binds) = match row_data.row_type {
            RowType::Insert => self.get_insert_query(&row_data)?,
            RowType::Update => self.get_update_query(&row_data)?,
            RowType::Delete => self.get_delete_query(&row_data)?,
        };
        Ok((sql, cols, binds))
    }

    pub fn get_batch_insert_query<'a>(
        &self,
        data: &'a Vec<RowData>,
        start_index: usize,
        batch_size: usize,
    ) -> Result<(String, Vec<String>, Vec<Option<&'a ColValue>>), Error> {
        let mut col_values = Vec::new();
        for _ in self.cols.iter() {
            col_values.push("?");
        }
        let col_values_str = format!("({})", col_values.join(","));

        let mut row_values = Vec::new();
        for _ in 0..batch_size {
            row_values.push(col_values_str.as_str());
        }

        let sql = format!(
            "REPLACE INTO {}.{}({}) VALUES{}",
            self.schema,
            self.tb,
            self.cols.join(","),
            row_values.join(",")
        );

        let mut cols = Vec::new();
        let mut binds = Vec::new();
        for i in start_index..start_index + batch_size {
            let row_data = &data[i];
            let after = row_data.after.as_ref().unwrap();
            for col_name in self.cols.iter() {
                cols.push(col_name.clone());
                binds.push(after.get(col_name));
            }
        }
        Ok((sql, cols, binds))
    }

    pub fn check_result(
        &self,
        actual_rows_affected: u64,
        expect_rows_affected: u64,
        sql: &str,
        row_data: &RowData,
    ) -> Result<(), Error> {
        if actual_rows_affected != expect_rows_affected {
            info!(
                "sql: {}\nrows_affected: {},rows_affected_expected: {}\n{}",
                sql,
                actual_rows_affected,
                expect_rows_affected,
                row_data.to_string(&self.cols)
            );
        }
        Ok(())
    }

    fn get_insert_query<'a>(
        &self,
        row_data: &'a RowData,
    ) -> Result<(String, Vec<String>, Vec<Option<&'a ColValue>>), Error> {
        let mut col_values = Vec::new();
        for _ in self.cols.iter() {
            col_values.push("?");
        }

        let sql = format!(
            "REPLACE INTO {}.{}({}) VALUES({})",
            self.schema,
            self.tb,
            self.cols.join(","),
            col_values.join(",")
        );

        let mut cols = Vec::new();
        let mut binds = Vec::new();
        let after = row_data.after.as_ref().unwrap();
        for col_name in self.cols.iter() {
            cols.push(col_name.clone());
            binds.push(after.get(col_name));
        }
        Ok((sql, cols, binds))
    }

    fn get_delete_query<'a>(
        &self,
        row_data: &'a RowData,
    ) -> Result<(String, Vec<String>, Vec<Option<&'a ColValue>>), Error> {
        let before = row_data.before.as_ref().unwrap();
        let (where_sql, not_null_cols) = self.get_where_info(&before)?;
        let mut sql = format!(
            "DELETE FROM {}.{} WHERE {}",
            self.schema, self.tb, where_sql
        );
        if self.key_map.is_empty() {
            sql += " LIMIT 1";
        }

        let mut cols = Vec::new();
        let mut binds = Vec::new();
        for col_name in not_null_cols.iter() {
            cols.push(col_name.clone());
            binds.push(before.get(col_name));
        }
        Ok((sql, cols, binds))
    }

    fn get_update_query<'a>(
        &self,
        row_data: &'a RowData,
    ) -> Result<(String, Vec<String>, Vec<Option<&'a ColValue>>), Error> {
        let before = row_data.before.as_ref().unwrap();
        let after = row_data.after.as_ref().unwrap();

        let mut set_cols = Vec::new();
        let mut set_pairs = Vec::new();
        for (col_name, _) in after.iter() {
            set_cols.push(col_name.clone());
            set_pairs.push(format!("{}=?", col_name));
        }

        let (where_sql, not_null_cols) = self.get_where_info(&before)?;
        let mut sql = format!(
            "UPDATE {}.{} SET {} WHERE {}",
            self.schema,
            self.tb,
            set_pairs.join(","),
            where_sql,
        );
        if self.key_map.is_empty() {
            sql += " LIMIT 1";
        }

        let mut cols = set_cols.clone();
        let mut binds = Vec::new();
        for col_name in set_cols.iter() {
            binds.push(after.get(col_name));
        }
        for col_name in not_null_cols.iter() {
            cols.push(col_name.clone());
            binds.push(before.get(col_name));
        }
        Ok((sql, cols, binds))
    }

    fn get_where_info(
        &self,
        col_value_map: &HashMap<String, ColValue>,
    ) -> Result<(String, Vec<String>), Error> {
        let mut where_sql = "".to_string();
        let mut not_null_cols = Vec::new();

        for col_name in self.where_cols.iter() {
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
