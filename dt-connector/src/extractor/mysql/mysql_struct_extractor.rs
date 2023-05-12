use std::{collections::HashMap, sync::atomic::AtomicBool};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::{
    error::Error,
    log_info,
    meta::{
        ddl_data::DdlData,
        ddl_type::DdlType,
        struct_meta::database_model::{Column, IndexColumn, IndexKind, StructModel},
    },
};

use futures::TryStreamExt;
use sqlx::{mysql::MySqlRow, MySql, Pool, Row};

use dt_common::meta::dt_data::DtData;

use crate::{
    extractor::{base_extractor::BaseExtractor, rdb_filter::RdbFilter},
    Extractor,
};

pub struct MysqlStructExtractor<'a> {
    pub conn_pool: Pool<MySql>,
    pub buffer: &'a ConcurrentQueue<DtData>,
    pub db: String,
    pub filter: RdbFilter,
    pub shut_down: &'a AtomicBool,
}

#[async_trait]
impl Extractor for MysqlStructExtractor<'_> {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!("MysqlStructExtractor starts, schema: {}", self.db,);
        self.extract_internal().await
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl MysqlStructExtractor<'_> {
    pub async fn extract_internal(&mut self) -> Result<(), Error> {
        let mut metas = Vec::new();
        for (_, meta) in self.get_table().await.unwrap() {
            metas.push(meta);
        }

        for (_, meta) in self.get_index().await.unwrap() {
            metas.push(meta);
        }

        for meta in metas {
            let ddl_data = DdlData {
                schema: self.db.clone(),
                query: String::new(),
                meta: Some(meta),
                ddl_type: DdlType::Unknown,
            };
            BaseExtractor::push_dt_data(&self.buffer, DtData::Ddl { ddl_data })
                .await
                .unwrap();
        }
        BaseExtractor::wait_task_finish(self.buffer, self.shut_down).await
    }

    // Create Table: https://dev.mysql.com/doc/refman/8.0/en/create-table.html
    async fn get_table(&mut self) -> Result<HashMap<String, StructModel>, Error> {
        let sql = format!("select t.table_schema,t.table_name,t.engine,t.table_comment,c.column_name,c.ordinal_position,c.column_default,c.is_nullable,c.column_type,c.column_key,c.extra,c.column_comment,c.character_set_name,c.collation_name
            from information_schema.tables t left join information_schema.columns c on t.table_schema = c.table_schema and t.table_name = c.table_name where t.table_schema ='{}'",
            self.db);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        let mut results = HashMap::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let (db, tb): (String, String) = (
                Self::get_str_with_null(&row, "TABLE_SCHEMA").unwrap(),
                Self::get_str_with_null(&row, "TABLE_NAME").unwrap(),
            );

            if self.filter.filter_tb(&db, &tb) {
                continue;
            }

            let full_tb_name = format!("{}.{}", db, tb);
            let engine_name = Self::get_str_with_null(&row, "ENGINE").unwrap();
            let table_comment = Self::get_str_with_null(&row, "TABLE_COMMENT").unwrap();
            let column = Column {
                column_name: Self::get_str_with_null(&row, "COLUMN_NAME").unwrap(),
                order_position: row.try_get("ORDINAL_POSITION").unwrap(),
                default_value: row.get("COLUMN_DEFAULT"),
                is_nullable: Self::get_str_with_null(&row, "IS_NULLABLE").unwrap(),
                column_type: Self::get_str_with_null(&row, "COLUMN_TYPE").unwrap(),
                column_key: Self::get_str_with_null(&row, "COLUMN_KEY").unwrap(),
                extra: Self::get_str_with_null(&row, "EXTRA").unwrap(),
                column_comment: Self::get_str_with_null(&row, "COLUMN_COMMENT").unwrap(),
                character_set: Self::get_str_with_null(&row, "CHARACTER_SET_NAME").unwrap(),
                collation: Self::get_str_with_null(&row, "COLLATION_NAME").unwrap(),
                generated: None,
            };

            if let Some(model) = results.get_mut(&full_tb_name) {
                match model {
                    StructModel::TableModel { columns, .. } => {
                        columns.push(column);
                    }
                    _ => {}
                }
            } else {
                results.insert(
                    full_tb_name,
                    StructModel::TableModel {
                        database_name: db.clone(),
                        schema_name: db,
                        table_name: tb,
                        engine_name,
                        table_comment,
                        columns: vec![column],
                    },
                );
            }
        }
        Ok(results)
    }

    // Create Index: https://dev.mysql.com/doc/refman/8.0/en/create-index.html
    async fn get_index(&mut self) -> Result<HashMap<String, StructModel>, Error> {
        let sql = format!("select table_schema,table_name,non_unique,index_name, seq_in_index,column_name,index_type,is_visible,comment from information_schema.statistics 
            WHERE index_name != 'PRIMARY' and table_schema ='{}' 
            ORDER BY table_schema, table_name, index_name", 
            self.db);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        let mut results: HashMap<String, StructModel> = HashMap::new();

        while let Some(row) = rows.try_next().await.unwrap() {
            let (db, tb, index_name): (String, String, String) = (
                Self::get_str_with_null(&row, "TABLE_SCHEMA").unwrap(),
                Self::get_str_with_null(&row, "TABLE_NAME").unwrap(),
                Self::get_str_with_null(&row, "INDEX_NAME").unwrap(),
            );

            if self.filter.filter_tb(&db, &tb) {
                continue;
            }

            let full_index_name = format!("{}.{}.{}", db, tb, index_name);
            let column = IndexColumn {
                column_name: Self::get_str_with_null(&row, "COLUMN_NAME").unwrap(),
                seq_in_index: row.try_get("SEQ_IN_INDEX")?,
            };

            if let Some(model) = results.get_mut(&full_index_name) {
                match model {
                    StructModel::IndexModel { columns, .. } => {
                        columns.push(column);
                    }
                    _ => {}
                }
            } else {
                results.insert(
                    full_index_name,
                    StructModel::IndexModel {
                        database_name: Self::get_str_with_null(&row, "TABLE_SCHEMA").unwrap(),
                        schema_name: String::from(""),
                        table_name: Self::get_str_with_null(&row, "TABLE_NAME").unwrap(),
                        index_name: Self::get_str_with_null(&row, "INDEX_NAME").unwrap(),
                        index_kind: self
                            .build_index_kind(
                                row.try_get("NON_UNIQUE")?,
                                Self::get_str_with_null(&row, "INDEX_NAME")
                                    .unwrap()
                                    .as_str(),
                            )
                            .unwrap(),
                        index_type: Self::get_str_with_null(&row, "INDEX_TYPE").unwrap(),
                        comment: Self::get_str_with_null(&row, "COMMENT").unwrap(),
                        tablespace: String::from(""),
                        definition: String::from(""),
                        columns: vec![column],
                    },
                );
            }
        }
        Ok(results)
    }

    fn build_index_kind(&self, non_unique: i32, index_name: &str) -> Result<IndexKind, Error> {
        if index_name == "PRIMARY" && non_unique == 0 {
            Ok(IndexKind::PrimaryKey)
        } else if non_unique == 0 {
            Ok(IndexKind::Unique)
        } else {
            Ok(IndexKind::Index)
        }
    }

    fn get_str_with_null(row: &MySqlRow, col_name: &str) -> Result<String, Error> {
        let mut str_val = String::from("");
        let str_val_option = row.get(col_name);
        match str_val_option {
            Some(s) => str_val = s,
            None => {}
        }
        Ok(str_val)
    }
}
