use std::collections::HashMap;

use crate::config::config_enums::DbType;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{mysql::MySqlRow, postgres::PgRow};

use crate::meta::adaptor::{
    mysql_col_value_convertor::MysqlColValueConvertor, pg_col_value_convertor::PgColValueConvertor,
};

use super::{
    col_value::ColValue, mysql::mysql_tb_meta::MysqlTbMeta, pg::pg_tb_meta::PgTbMeta,
    rdb_tb_meta::RdbTbMeta, row_type::RowType,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RowData {
    pub schema: String,
    pub tb: String,
    pub row_type: RowType,
    pub before: Option<HashMap<String, ColValue>>,
    pub after: Option<HashMap<String, ColValue>>,
    pub data_size: usize,
}

impl ToString for RowData {
    fn to_string(&self) -> String {
        json!(self).to_string()
    }
}

impl RowData {
    pub fn new(
        schema: String,
        tb: String,
        row_type: RowType,
        before: Option<HashMap<String, ColValue>>,
        after: Option<HashMap<String, ColValue>>,
    ) -> Self {
        let mut me = Self {
            schema,
            tb,
            row_type,
            before,
            after,
            data_size: 0,
        };
        me.data_size = me.get_data_malloc_size();
        me
    }

    pub fn from_mysql_row(row: &MySqlRow, tb_meta: &MysqlTbMeta) -> Self {
        Self::from_mysql_compatible_row(row, tb_meta, &DbType::Mysql)
    }

    pub fn from_mysql_compatible_row(
        row: &MySqlRow,
        tb_meta: &MysqlTbMeta,
        db_type: &DbType,
    ) -> Self {
        let mut after = HashMap::new();
        for (col, col_type) in &tb_meta.col_type_map {
            let col_val =
                MysqlColValueConvertor::from_query_mysql_compatible(row, col, col_type, db_type)
                    .unwrap();
            after.insert(col.to_string(), col_val);
        }
        Self::build_insert_row_data(after, &tb_meta.basic)
    }

    pub fn from_pg_row(row: &PgRow, tb_meta: &PgTbMeta) -> Self {
        let mut after = HashMap::new();
        for (col_name, col_type) in &tb_meta.col_type_map {
            let col_value = PgColValueConvertor::from_query(row, col_name, col_type).unwrap();
            after.insert(col_name.to_string(), col_value);
        }
        Self::build_insert_row_data(after, &tb_meta.basic)
    }

    pub fn build_insert_row_data(after: HashMap<String, ColValue>, tb_meta: &RdbTbMeta) -> Self {
        Self::new(
            tb_meta.schema.clone(),
            tb_meta.tb.clone(),
            RowType::Insert,
            None,
            Some(after),
        )
    }

    pub fn get_hash_code(&self, tb_meta: &RdbTbMeta) -> u128 {
        let col_values = match self.row_type {
            RowType::Insert => self.after.as_ref().unwrap(),
            _ => self.before.as_ref().unwrap(),
        };

        // refer to: https://docs.oracle.com/javase/6/docs/api/java/util/List.html#hashCode%28%29
        let mut hash_code = 1u128;
        for col in tb_meta.id_cols.iter() {
            let col_hash_code = col_values.get(col).unwrap().hash_code();
            // col_hash_code is 0 if col_value is ColValue::None,
            // consider fowlling case,
            // create table a(id int, value int, unique key(id, value));
            // insert into a values(1, NULL);
            // delete from a where (id, value) in ((1, NULL));  // this won't work
            // delete from a where id=1 and value is NULL;  // this works
            // so here return 0 to stop merging to avoid batch deleting
            if col_hash_code == 0 {
                return 0;
            }
            hash_code = 31 * hash_code + col_hash_code as u128;
        }
        hash_code
    }

    pub fn refresh_data_size(&mut self) {
        self.data_size = self.get_data_malloc_size();
    }

    fn get_data_malloc_size(&self) -> usize {
        let mut size = 0;
        // do not use mem::size_of_val() since:
        // for Pointer: it returns the size of pointer without the pointed data
        // for HashMap and Vector: it returns the size of the structure without the stored items
        if let Some(col_values) = &self.before {
            for (_, v) in col_values.iter() {
                size += v.get_malloc_size();
            }
        }
        if let Some(col_values) = &self.after {
            for (_, v) in col_values.iter() {
                size += v.get_malloc_size();
            }
        }
        // ignore other fields
        size
    }
}
