use std::collections::HashMap;

use dt_common::{error::Error, utils::rdb_filter::RdbFilter};
use dt_meta::struct_meta::database_model::{Column, IndexColumn, IndexKind, StructModel};
use futures::TryStreamExt;
use sqlx::{mysql::MySqlRow, MySql, Pool, Row};

pub struct MysqlStructFetcher {
    pub conn_pool: Pool<MySql>,
    pub db: String,
    pub filter: Option<RdbFilter>,
}

impl MysqlStructFetcher {
    // Create Table: https://dev.mysql.com/doc/refman/8.0/en/create-table.html
    pub async fn get_table(
        &mut self,
        struct_model: &Option<StructModel>,
    ) -> Result<HashMap<String, StructModel>, Error> {
        let struct_model = match struct_model {
            Some(model) => model.clone(),
            None => StructModel::TableModel {
                database_name: self.db.clone(),
                schema_name: String::from(""),
                table_name: String::from(""),
                engine_name: String::from(""),
                table_comment: String::from(""),
                columns: vec![],
            },
        };
        let sql = self.sql_builder(&struct_model);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        let mut results = HashMap::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let (db, tb): (String, String) = (
                Self::get_str_with_null(&row, "TABLE_SCHEMA").unwrap(),
                Self::get_str_with_null(&row, "TABLE_NAME").unwrap(),
            );

            if let Some(filter) = &mut self.filter {
                if filter.filter_tb(&db, &tb) {
                    continue;
                }
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
                if let StructModel::TableModel { columns, .. } = model {
                    columns.push(column);
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
    pub async fn get_index(
        &mut self,
        struct_model: &Option<StructModel>,
    ) -> Result<HashMap<String, StructModel>, Error> {
        let struct_model = match struct_model {
            Some(model) => model.clone(),
            None => StructModel::IndexModel {
                database_name: self.db.clone(),
                schema_name: String::from(""),
                table_name: String::from(""),
                index_name: String::from(""),
                index_kind: IndexKind::Unkown,
                index_type: String::from(""),
                comment: String::from(""),
                tablespace: String::from(""),
                definition: String::from(""),
                columns: vec![],
            },
        };
        let sql = self.sql_builder(&struct_model);

        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        let mut results: HashMap<String, StructModel> = HashMap::new();

        while let Some(row) = rows.try_next().await.unwrap() {
            let (db, tb, index_name): (String, String, String) = (
                Self::get_str_with_null(&row, "TABLE_SCHEMA").unwrap(),
                Self::get_str_with_null(&row, "TABLE_NAME").unwrap(),
                Self::get_str_with_null(&row, "INDEX_NAME").unwrap(),
            );

            if let Some(filter) = &mut self.filter {
                if filter.filter_tb(&db, &tb) {
                    continue;
                }
            }

            let full_index_name = format!("{}.{}.{}", db, tb, index_name);
            let column = IndexColumn {
                column_name: Self::get_str_with_null(&row, "COLUMN_NAME").unwrap(),
                seq_in_index: row.try_get("SEQ_IN_INDEX")?,
            };

            if let Some(model) = results.get_mut(&full_index_name) {
                if let StructModel::IndexModel { columns, .. } = model {
                    columns.push(column);
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

    pub async fn fetch_with_model(
        mut self,
        struct_model: &StructModel,
    ) -> Result<Option<StructModel>, Error> {
        let model_option = Some(struct_model.to_owned());
        let result = match struct_model {
            StructModel::TableModel { .. } => self.get_table(&model_option).await,
            StructModel::IndexModel { .. } => self.get_index(&model_option).await,
            _ => {
                let result: HashMap<String, StructModel> = HashMap::new();
                Ok(result)
            }
        };
        match result {
            Ok(r) => {
                let result_option = if let Some(first_model) = r.values().next() {
                    Some(first_model.to_owned())
                } else {
                    None
                };
                Ok(result_option)
            }
            Err(e) => Err(e),
        }
    }

    fn sql_builder(&self, struct_model: &StructModel) -> String {
        let sql: String = match struct_model {
            StructModel::TableModel {
                database_name,
                schema_name: _,
                table_name,
                engine_name: _,
                table_comment: _,
                columns: _,
            } => {
                let mut s = format!("select t.table_schema,t.table_name,t.engine,t.table_comment,c.column_name,c.ordinal_position,c.column_default,c.is_nullable,c.column_type,c.column_key,c.extra,c.column_comment,c.character_set_name,c.collation_name 
from information_schema.tables t left join information_schema.columns c on t.table_schema = c.table_schema and t.table_name = c.table_name where t.table_schema ='{}'",database_name);
                if !table_name.is_empty() {
                    s = format!("{} and t.table_name = '{}' ", s, table_name);
                }
                s
            }
            StructModel::IndexModel {
                database_name,
                schema_name: _,
                table_name: _,
                index_name,
                index_kind: _,
                index_type: _,
                comment: _,
                tablespace: _,
                definition: _,
                columns: _,
            } => {
                let mut s = format!("select table_schema,table_name,non_unique,index_name, seq_in_index,column_name,index_type,is_visible,comment from information_schema.statistics 
WHERE index_name != 'PRIMARY' and table_schema ='{}'", database_name);
                if !index_name.is_empty() {
                    s = format!("{} and index_name = '{}' ", s, index_name);
                }
                format!("{} ORDER BY table_schema, table_name, index_name ", s)
            }
            _ => String::from(""),
        };

        sql
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
        if let Some(s) = str_val_option {
            str_val = s;
        }
        Ok(str_val)
    }
}
