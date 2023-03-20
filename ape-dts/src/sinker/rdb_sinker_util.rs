use std::collections::HashMap;

use log::info;

use crate::{
    error::Error,
    meta::{
        col_value::ColValue, mysql::mysql_tb_meta::MysqlTbMeta, pg::pg_tb_meta::PgTbMeta,
        row_data::RowData, row_type::RowType,
    },
};

pub struct RdbSinkerUtil<'a> {
    schema: &'a str,
    tb: &'a str,
    cols: &'a Vec<String>,
    where_cols: &'a Vec<String>,
    key_map: &'a HashMap<String, Vec<String>>,
    pg_tb_meta: Option<&'a PgTbMeta>,
}

impl RdbSinkerUtil<'_> {
    #[inline(always)]
    pub fn new_for_mysql<'a>(tb_meta: &'a MysqlTbMeta) -> RdbSinkerUtil {
        RdbSinkerUtil {
            schema: &tb_meta.db,
            tb: &tb_meta.tb,
            cols: &tb_meta.cols,
            where_cols: &tb_meta.where_cols,
            key_map: &tb_meta.key_map,
            pg_tb_meta: Option::None,
        }
    }

    #[inline(always)]
    pub fn new_for_pg<'a>(tb_meta: &'a PgTbMeta) -> RdbSinkerUtil {
        RdbSinkerUtil {
            schema: &tb_meta.schema,
            tb: &tb_meta.tb,
            cols: &tb_meta.cols,
            where_cols: &tb_meta.where_cols,
            key_map: &tb_meta.key_map,
            pg_tb_meta: Some(&tb_meta),
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

    pub fn get_batch_delete_query<'a>(
        &self,
        data: &'a Vec<RowData>,
        start_index: usize,
        batch_size: usize,
    ) -> Result<(String, Vec<String>, Vec<Option<&'a ColValue>>), Error> {
        let mut all_placeholders = Vec::new();
        let mut placeholder_index = 1;
        for _ in 0..batch_size {
            let mut placeholders = Vec::new();
            for col in self.where_cols.iter() {
                placeholders.push(self.get_placeholder(placeholder_index, col));
                placeholder_index += 1;
            }
            all_placeholders.push(format!("({})", placeholders.join(",")));
        }

        let sql = format!(
            "DELETE FROM {}.{} WHERE ({}) IN ({})",
            self.schema,
            self.tb,
            self.where_cols.join(","),
            all_placeholders.join(",")
        );

        let mut cols = Vec::new();
        let mut binds = Vec::new();
        for i in start_index..start_index + batch_size {
            let row_data = &data[i];
            let before = row_data.before.as_ref().unwrap();
            for col in self.where_cols.iter() {
                cols.push(col.clone());
                let col_value = before.get(col);
                if *col_value.unwrap() == ColValue::None {
                    return Err(Error::Unexpected {
                        error: format!(
                            "db: {}, tb: {}, where col: {} is NULL, which should not happen in batch delete",
                            self.schema, self.tb, col
                        ),
                    });
                }
                binds.push(col_value);
            }
        }
        Ok((sql, cols, binds))
    }

    pub fn get_batch_insert_query<'a>(
        &self,
        data: &'a Vec<RowData>,
        start_index: usize,
        batch_size: usize,
    ) -> Result<(String, Vec<String>, Vec<Option<&'a ColValue>>), Error> {
        let mut placeholder_index = 1;
        let mut row_values = Vec::new();
        for _ in 0..batch_size {
            let mut col_values = Vec::new();
            for col in self.cols.iter() {
                col_values.push(self.get_placeholder(placeholder_index, col));
                placeholder_index += 1;
            }
            row_values.push(format!("({})", col_values.join(",")));
        }

        let sql = format!(
            "INSERT INTO {}.{}({}) VALUES{}",
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

    pub fn get_insert_query<'a>(
        &self,
        row_data: &'a RowData,
    ) -> Result<(String, Vec<String>, Vec<Option<&'a ColValue>>), Error> {
        let mut col_values = Vec::new();
        for i in 0..self.cols.len() {
            col_values.push(self.get_placeholder(i + 1, &self.cols[i]));
        }

        let sql = format!(
            "INSERT INTO {}.{}({}) VALUES({})",
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
        let (where_sql, not_null_cols) = self.get_where_info(1, &before)?;
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

        let mut placeholder_index = 1;
        let mut set_cols = Vec::new();
        let mut set_pairs = Vec::new();
        for (col, _) in after.iter() {
            set_cols.push(col.clone());
            set_pairs.push(format!(
                "{}={}",
                col,
                self.get_placeholder(placeholder_index, col)
            ));
            placeholder_index += 1;
        }

        if set_pairs.is_empty() {
            return Err(Error::Unexpected {
                error: format!(
                    "db: {}, tb: {}, no cols in after, which should not happen in update",
                    self.schema, self.tb
                ),
            });
        }

        let (where_sql, not_null_cols) = self.get_where_info(placeholder_index, &before)?;
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
        mut placeholder_index: usize,
        col_value_map: &HashMap<String, ColValue>,
    ) -> Result<(String, Vec<String>), Error> {
        let mut where_sql = "".to_string();
        let mut not_null_cols = Vec::new();

        for col in self.where_cols.iter() {
            if !where_sql.is_empty() {
                where_sql += " AND";
            }

            let col_value = col_value_map.get(col);
            if let Some(value) = col_value {
                if *value == ColValue::None {
                    where_sql = format!("{} {} IS NULL", where_sql, col);
                } else {
                    where_sql = format!(
                        "{} {} = {}",
                        where_sql,
                        col,
                        self.get_placeholder(placeholder_index, col)
                    );
                    not_null_cols.push(col.clone());
                }
            } else {
                where_sql = format!("{} {} IS NULL", where_sql, col);
            }

            placeholder_index += 1;
        }
        Ok((where_sql, not_null_cols))
    }

    #[inline(always)]
    pub fn get_placeholder(&self, index: usize, col: &str) -> String {
        if let Some(tb_meta) = self.pg_tb_meta {
            let col_type = tb_meta.col_type_map.get(col).unwrap();
            // workaround for types like bit(3)
            let col_type_name = if col_type.short_name == "bit" {
                "varbit"
            } else {
                &col_type.short_name
            };
            return format!("${}::{}", index, col_type_name);
        }

        "?".to_string()
    }

    #[inline(always)]
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
}
