use std::collections::HashMap;

use dt_common::{config::config_enums::DbType, error::Error, utils::sql_util::SqlUtil};
use dt_meta::{
    adaptor::{
        pg_col_value_convertor::PgColValueConvertor,
        sqlx_ext::{SqlxMysqlExt, SqlxPgExt},
    },
    col_value::ColValue,
    mysql::mysql_tb_meta::MysqlTbMeta,
    pg::pg_tb_meta::PgTbMeta,
    rdb_tb_meta::RdbTbMeta,
    row_data::RowData,
    row_type::RowType,
};
use sqlx::{mysql::MySqlArguments, postgres::PgArguments, query::Query, MySql, Postgres};

pub struct RdbQueryInfo<'a> {
    pub sql: String,
    pub cols: Vec<String>,
    pub binds: Vec<Option<&'a ColValue>>,
}

pub struct RdbQueryBuilder<'a> {
    rdb_tb_meta: &'a RdbTbMeta,
    db_type: DbType,
    pg_tb_meta: Option<&'a PgTbMeta>,
    mysql_tb_meta: Option<&'a MysqlTbMeta>,
}

impl RdbQueryBuilder<'_> {
    #[inline(always)]
    pub fn new_for_mysql(tb_meta: &MysqlTbMeta) -> RdbQueryBuilder {
        RdbQueryBuilder {
            rdb_tb_meta: &tb_meta.basic,
            pg_tb_meta: Option::None,
            mysql_tb_meta: Some(tb_meta),
            db_type: DbType::Mysql,
        }
    }

    #[inline(always)]
    pub fn new_for_pg(tb_meta: &PgTbMeta) -> RdbQueryBuilder {
        RdbQueryBuilder {
            rdb_tb_meta: &tb_meta.basic,
            pg_tb_meta: Some(tb_meta),
            mysql_tb_meta: None,
            db_type: DbType::Pg,
        }
    }

    #[inline(always)]
    pub fn create_mysql_query<'a>(
        &self,
        query_info: &'a RdbQueryInfo,
    ) -> Query<'a, MySql, MySqlArguments> {
        let mut query: Query<MySql, MySqlArguments> = sqlx::query(&query_info.sql);
        for i in 0..query_info.binds.len() {
            let col_type = self
                .mysql_tb_meta
                .unwrap()
                .col_type_map
                .get(&query_info.cols[i])
                .unwrap();
            query = query.bind_col_value(query_info.binds[i], col_type);
        }
        query
    }

    #[inline(always)]
    pub fn create_pg_query<'a>(
        &self,
        query_info: &'a RdbQueryInfo,
    ) -> Query<'a, Postgres, PgArguments> {
        let mut query: Query<Postgres, PgArguments> = sqlx::query(&query_info.sql);
        for i in 0..query_info.binds.len() {
            let col_type = self
                .pg_tb_meta
                .unwrap()
                .col_type_map
                .get(&query_info.cols[i])
                .unwrap();
            query = query.bind_col_value(query_info.binds[i], col_type);
        }
        query
    }

    #[allow(clippy::type_complexity)]
    pub fn get_query_info<'a>(&self, row_data: &'a RowData) -> Result<RdbQueryInfo<'a>, Error> {
        match row_data.row_type {
            RowType::Insert => self.get_insert_query(row_data),
            RowType::Update => self.get_update_query(row_data),
            RowType::Delete => self.get_delete_query(row_data),
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn get_batch_delete_query<'a>(
        &self,
        data: &'a [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> Result<(RdbQueryInfo<'a>, usize), Error> {
        let mut data_size = 0;
        let mut all_placeholders = Vec::new();
        let mut placeholder_index = 1;
        for _ in 0..batch_size {
            let mut placeholders = Vec::new();
            for col in self.rdb_tb_meta.id_cols.iter() {
                placeholders.push(self.get_placeholder(placeholder_index, col));
                placeholder_index += 1;
            }
            all_placeholders.push(format!("({})", placeholders.join(",")));
        }

        let sql = format!(
            "DELETE FROM {}.{} WHERE ({}) IN ({})",
            self.escape(&self.rdb_tb_meta.schema),
            self.escape(&self.rdb_tb_meta.tb),
            self.escape_cols(&self.rdb_tb_meta.id_cols).join(","),
            all_placeholders.join(",")
        );

        let mut cols = Vec::new();
        let mut binds = Vec::new();
        for row_data in data.iter().skip(start_index).take(batch_size) {
            data_size += row_data.data_size;
            let before = row_data.before.as_ref().unwrap();
            for col in self.rdb_tb_meta.id_cols.iter() {
                cols.push(col.clone());
                let col_value = before.get(col);
                if *col_value.unwrap() == ColValue::None {
                    return Err(Error::Unexpected(format!(
                            "db: {}, tb: {}, where col: {} is NULL, which should not happen in batch delete",
                            self.rdb_tb_meta.schema, self.rdb_tb_meta.tb, col
                        )));
                }
                binds.push(col_value);
            }
        }
        Ok((RdbQueryInfo { sql, cols, binds }, data_size))
    }

    #[allow(clippy::type_complexity)]
    pub fn get_batch_insert_query<'a>(
        &self,
        data: &'a [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> Result<(RdbQueryInfo<'a>, usize), Error> {
        let mut malloc_size = 0;
        let mut placeholder_index = 1;
        let mut row_values = Vec::new();
        for _ in 0..batch_size {
            let mut col_values = Vec::new();
            for col in self.rdb_tb_meta.cols.iter() {
                col_values.push(self.get_placeholder(placeholder_index, col));
                placeholder_index += 1;
            }
            row_values.push(format!("({})", col_values.join(",")));
        }

        let sql = format!(
            "INSERT INTO {}.{}({}) VALUES{}",
            self.escape(&self.rdb_tb_meta.schema),
            self.escape(&self.rdb_tb_meta.tb),
            self.escape_cols(&self.rdb_tb_meta.cols).join(","),
            row_values.join(",")
        );

        let mut cols = Vec::new();
        let mut binds = Vec::new();
        for row_data in data.iter().skip(start_index).take(batch_size) {
            malloc_size += row_data.data_size;
            let after = row_data.after.as_ref().unwrap();
            for col_name in self.rdb_tb_meta.cols.iter() {
                cols.push(col_name.clone());
                binds.push(after.get(col_name));
            }
        }
        Ok((RdbQueryInfo { sql, cols, binds }, malloc_size))
    }

    #[allow(clippy::type_complexity)]
    pub fn get_insert_query<'a>(&self, row_data: &'a RowData) -> Result<RdbQueryInfo<'a>, Error> {
        let mut col_values = Vec::new();
        for i in 0..self.rdb_tb_meta.cols.len() {
            col_values.push(self.get_placeholder(i + 1, &self.rdb_tb_meta.cols[i]));
        }

        let sql = format!(
            "INSERT INTO {}.{}({}) VALUES({})",
            self.escape(&self.rdb_tb_meta.schema),
            self.escape(&self.rdb_tb_meta.tb),
            self.escape_cols(&self.rdb_tb_meta.cols).join(","),
            col_values.join(",")
        );

        let mut cols = Vec::new();
        let mut binds = Vec::new();
        let after = row_data.after.as_ref().unwrap();
        for col_name in self.rdb_tb_meta.cols.iter() {
            cols.push(col_name.clone());
            binds.push(after.get(col_name));
        }
        Ok(RdbQueryInfo { sql, cols, binds })
    }

    #[allow(clippy::type_complexity)]
    fn get_delete_query<'a>(&self, row_data: &'a RowData) -> Result<RdbQueryInfo<'a>, Error> {
        let before = row_data.before.as_ref().unwrap();
        let (where_sql, not_null_cols) = self.get_where_info(1, before)?;
        let mut sql = format!(
            "DELETE FROM {}.{} WHERE {}",
            self.escape(&self.rdb_tb_meta.schema),
            self.escape(&self.rdb_tb_meta.tb),
            where_sql
        );
        if self.rdb_tb_meta.key_map.is_empty() {
            sql += " LIMIT 1";
        }

        let mut cols = Vec::new();
        let mut binds = Vec::new();
        for col_name in not_null_cols.iter() {
            cols.push(col_name.clone());
            binds.push(before.get(col_name));
        }
        Ok(RdbQueryInfo { sql, cols, binds })
    }

    #[allow(clippy::type_complexity)]
    fn get_update_query<'a>(&self, row_data: &'a RowData) -> Result<RdbQueryInfo<'a>, Error> {
        let before = row_data.before.as_ref().unwrap();
        let after = row_data.after.as_ref().unwrap();

        let mut placeholder_index = 1;
        let mut set_cols = Vec::new();
        let mut set_pairs = Vec::new();
        for (col, _) in after.iter() {
            set_cols.push(col.clone());
            set_pairs.push(format!(
                "{}={}",
                self.escape(col),
                self.get_placeholder(placeholder_index, col)
            ));
            placeholder_index += 1;
        }

        if set_pairs.is_empty() {
            return Err(Error::Unexpected(format!(
                "db: {}, tb: {}, no cols in after, which should not happen in update",
                self.rdb_tb_meta.schema, self.rdb_tb_meta.tb
            )));
        }

        let (where_sql, not_null_cols) = self.get_where_info(placeholder_index, before)?;
        let mut sql = format!(
            "UPDATE {}.{} SET {} WHERE {}",
            self.escape(&self.rdb_tb_meta.schema),
            self.escape(&self.rdb_tb_meta.tb),
            set_pairs.join(","),
            where_sql,
        );
        if self.rdb_tb_meta.key_map.is_empty() {
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
        Ok(RdbQueryInfo { sql, cols, binds })
    }

    #[allow(clippy::type_complexity)]
    pub fn get_select_query<'a>(&self, row_data: &'a RowData) -> Result<RdbQueryInfo<'a>, Error> {
        let after = row_data.after.as_ref().unwrap();
        let (where_sql, not_null_cols) = self.get_where_info(1, after)?;
        let mut sql = format!(
            "SELECT {} FROM {}.{} WHERE {}",
            self.build_extract_cols_str()?,
            self.escape(&self.rdb_tb_meta.schema),
            self.escape(&self.rdb_tb_meta.tb),
            where_sql,
        );

        if self.rdb_tb_meta.key_map.is_empty() {
            sql += " LIMIT 1";
        }

        let mut cols = Vec::new();
        let mut binds = Vec::new();
        for col_name in not_null_cols.iter() {
            cols.push(col_name.clone());
            binds.push(after.get(col_name));
        }
        Ok(RdbQueryInfo { sql, cols, binds })
    }

    #[allow(clippy::type_complexity)]
    pub fn get_batch_select_query<'a>(
        &self,
        data: &'a [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> Result<RdbQueryInfo<'a>, Error> {
        let where_sql = self.get_where_in_info(batch_size)?;
        let sql = format!(
            "SELECT {} FROM {}.{} WHERE {}",
            self.build_extract_cols_str()?,
            self.escape(&self.rdb_tb_meta.schema),
            self.escape(&self.rdb_tb_meta.tb),
            where_sql,
        );

        let mut cols = Vec::new();
        let mut binds = Vec::new();
        for row_data in data.iter().skip(start_index).take(batch_size) {
            let after = row_data.after.as_ref().unwrap();
            for col in self.rdb_tb_meta.id_cols.iter() {
                cols.push(col.clone());
                let col_value = after.get(col);
                if *col_value.unwrap() == ColValue::None {
                    return Err(Error::Unexpected(format!(
                            "db: {}, tb: {}, where col: {} is NULL, which should not happen in batch select",
                            self.rdb_tb_meta.schema, self.rdb_tb_meta.tb, col
                        )));
                }
                binds.push(col_value);
            }
        }
        Ok(RdbQueryInfo { sql, cols, binds })
    }

    pub fn build_extract_cols_str(&self) -> Result<String, Error> {
        if let Some(tb_meta) = self.pg_tb_meta {
            let mut extract_cols = Vec::new();
            for col in self.rdb_tb_meta.cols.iter() {
                let col_type = tb_meta.col_type_map.get(col).unwrap();
                let extract_type = PgColValueConvertor::get_extract_type(col_type);
                let extract_col = if extract_type.is_empty() {
                    self.escape(col)
                } else {
                    format!("{}::{}", self.escape(col), extract_type)
                };
                extract_cols.push(extract_col);
            }
            return Ok(extract_cols.join(","));
        }
        Ok("*".to_string())
    }

    fn get_where_info(
        &self,
        mut placeholder_index: usize,
        col_value_map: &HashMap<String, ColValue>,
    ) -> Result<(String, Vec<String>), Error> {
        let mut where_sql = String::new();
        let mut not_null_cols = Vec::new();

        for col in self.rdb_tb_meta.id_cols.iter() {
            if !where_sql.is_empty() {
                where_sql += " AND";
            }

            let escaped_col = self.escape(col);
            let col_value = col_value_map.get(col);
            if let Some(value) = col_value {
                if *value == ColValue::None {
                    where_sql = format!("{} {} IS NULL", where_sql, escaped_col);
                } else {
                    where_sql = format!(
                        "{} {} = {}",
                        where_sql,
                        escaped_col,
                        self.get_placeholder(placeholder_index, col)
                    );
                    not_null_cols.push(col.clone());
                }
            } else {
                where_sql = format!("{} {} IS NULL", where_sql, escaped_col);
            }

            placeholder_index += 1;
        }
        Ok((where_sql, not_null_cols))
    }

    fn get_where_in_info(&self, batch_size: usize) -> Result<String, Error> {
        let mut all_placeholders = Vec::new();
        let mut placeholder_index = 1;
        for _ in 0..batch_size {
            let mut placeholders = Vec::new();
            for col in self.rdb_tb_meta.id_cols.iter() {
                placeholders.push(self.get_placeholder(placeholder_index, col));
                placeholder_index += 1;
            }
            all_placeholders.push(format!("({})", placeholders.join(",")));
        }

        Ok(format!(
            "({}) IN ({})",
            self.escape_cols(&self.rdb_tb_meta.id_cols).join(","),
            all_placeholders.join(",")
        ))
    }

    #[inline(always)]
    pub fn get_placeholder(&self, index: usize, col: &str) -> String {
        if let Some(tb_meta) = self.pg_tb_meta {
            let col_type = tb_meta.col_type_map.get(col).unwrap();
            // TODO: workaround for types like bit(3)
            let col_type_name = if col_type.short_name == "bit" {
                "varbit"
            } else {
                &col_type.short_name
            };
            return format!("${}::{}", index, col_type_name);
        }

        "?".to_string()
    }

    fn escape(&self, origin: &str) -> String {
        SqlUtil::escape_by_db_type(origin, &self.db_type)
    }

    fn escape_cols(&self, cols: &Vec<String>) -> Vec<String> {
        SqlUtil::escape_cols(cols, &self.db_type)
    }
}
