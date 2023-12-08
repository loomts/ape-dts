use std::collections::HashMap;

use dt_common::{error::Error, utils::rdb_filter::RdbFilter};
use dt_meta::{
    mysql::mysql_meta_manager::MysqlMetaManager,
    struct_meta::database_model::{Column, IndexColumn, IndexKind, StructModel},
};
use futures::TryStreamExt;
use sqlx::{mysql::MySqlRow, MySql, Pool, Row};

pub struct MysqlStructFetcher {
    pub conn_pool: Pool<MySql>,
    pub db: String,
    pub filter: Option<RdbFilter>,
    pub meta_manager: MysqlMetaManager,
}

const SCHEMA_NAME: &str = "SCHEMA_NAME";

const INDEX_NAME: &str = "INDEX_NAME";
const INDEX_TYPE: &str = "INDEX_TYPE";

const TABLE_SCHEMA: &str = "TABLE_SCHEMA";
const TABLE_NAME: &str = "TABLE_NAME";
const TABLE_COMMENT: &str = "TABLE_COMMENT";

const COLUMN_NAME: &str = "COLUMN_NAME";
const COLUMN_TYPE: &str = "COLUMN_TYPE";
const COLUMN_KEY: &str = "COLUMN_KEY";
const COLUMN_DEFAULT: &str = "COLUMN_DEFAULT";
const COLUMN_COMMENT: &str = "COLUMN_COMMENT";

const SEQ_IN_INDEX: &str = "SEQ_IN_INDEX";
const NON_UNIQUE: &str = "NON_UNIQUE";
const ORDINAL_POSITION: &str = "ORDINAL_POSITION";

const COMMENT: &str = "COMMENT";
const PRIMARY: &str = "PRIMARY";
const ENGINE: &str = "ENGINE";
const IS_NULLABLE: &str = "IS_NULLABLE";
const EXTRA: &str = "EXTRA";
const CHECK: &str = "CHECK";

const CHARACTER_SET_NAME: &str = "CHARACTER_SET_NAME";
const COLLATION_NAME: &str = "COLLATION_NAME";

const CONSTRAINT_SCHEMA: &str = "CONSTRAINT_SCHEMA";
const CONSTRAINT_NAME: &str = "CONSTRAINT_NAME";
const CHECK_CLAUSE: &str = "CHECK_CLAUSE";

impl MysqlStructFetcher {
    // Create Database: https://dev.mysql.com/doc/refman/8.0/en/create-database.html
    pub async fn get_database(
        &mut self,
        struct_model: &Option<StructModel>,
    ) -> Result<HashMap<String, StructModel>, Error> {
        let struct_model = match struct_model {
            Some(model) => model.clone(),
            None => StructModel::DatabaseModel {
                name: String::new(),
            },
        };

        let sql = self.sql_builder(&struct_model);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        let mut results = HashMap::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let db = Self::get_str_with_null(&row, SCHEMA_NAME).unwrap();

            if let Some(filter) = &mut self.filter {
                if filter.filter_db(&db) {
                    continue;
                }
            }
            results.insert(db.clone(), StructModel::DatabaseModel { name: db.clone() });
        }
        return Ok(results);
    }

    // Create Table: https://dev.mysql.com/doc/refman/8.0/en/create-table.html
    pub async fn get_table(
        &mut self,
        struct_model: &Option<StructModel>,
    ) -> Result<HashMap<String, StructModel>, Error> {
        let struct_model = match struct_model {
            Some(model) => model.clone(),
            None => StructModel::TableModel {
                database_name: self.db.clone(),
                schema_name: String::new(),
                table_name: String::new(),
                engine_name: String::new(),
                table_comment: String::new(),
                columns: vec![],
            },
        };
        let sql = self.sql_builder(&struct_model);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        let mut results = HashMap::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let (db, tb) = (
                Self::get_str_with_null(&row, TABLE_SCHEMA).unwrap(),
                Self::get_str_with_null(&row, TABLE_NAME).unwrap(),
            );

            if let Some(filter) = &mut self.filter {
                if filter.filter_tb(&db, &tb) {
                    continue;
                }
            }

            let full_tb_name = format!("{}.{}", db, tb);
            let engine_name = Self::get_str_with_null(&row, ENGINE).unwrap();
            let table_comment = Self::get_str_with_null(&row, TABLE_COMMENT).unwrap();
            let column = Column {
                column_name: Self::get_str_with_null(&row, COLUMN_NAME).unwrap(),
                order_position: row.try_get(ORDINAL_POSITION).unwrap(),
                default_value: row.get(COLUMN_DEFAULT),
                is_nullable: Self::get_str_with_null(&row, IS_NULLABLE).unwrap(),
                column_type: Self::get_str_with_null(&row, COLUMN_TYPE).unwrap(),
                column_key: Self::get_str_with_null(&row, COLUMN_KEY).unwrap(),
                extra: Self::get_str_with_null(&row, EXTRA).unwrap(),
                column_comment: Self::get_str_with_null(&row, COLUMN_COMMENT).unwrap(),
                character_set: Self::get_str_with_null(&row, CHARACTER_SET_NAME).unwrap(),
                collation: Self::get_str_with_null(&row, COLLATION_NAME).unwrap(),
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
                        schema_name: String::new(),
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
                schema_name: String::new(),
                table_name: String::new(),
                index_name: String::new(),
                index_kind: IndexKind::Unkown,
                index_type: String::new(),
                comment: String::new(),
                tablespace: String::new(),
                definition: String::new(),
                columns: vec![],
            },
        };
        let sql = self.sql_builder(&struct_model);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        let mut results: HashMap<String, StructModel> = HashMap::new();

        while let Some(row) = rows.try_next().await.unwrap() {
            let (db, tb, index_name) = (
                Self::get_str_with_null(&row, TABLE_SCHEMA).unwrap(),
                Self::get_str_with_null(&row, TABLE_NAME).unwrap(),
                Self::get_str_with_null(&row, INDEX_NAME).unwrap(),
            );

            if let Some(filter) = &mut self.filter {
                if filter.filter_tb(&db, &tb) {
                    continue;
                }
            }

            let full_index_name = format!("{}.{}.{}", db, tb, index_name);
            let column = IndexColumn {
                column_name: Self::get_str_with_null(&row, COLUMN_NAME).unwrap(),
                seq_in_index: {
                    if self.meta_manager.version.starts_with("5.") {
                        let seq_in_index: i32 = row.try_get(SEQ_IN_INDEX).unwrap();
                        seq_in_index as u32
                    } else {
                        row.try_get(SEQ_IN_INDEX).unwrap()
                    }
                },
            };

            if let Some(model) = results.get_mut(&full_index_name) {
                if let StructModel::IndexModel { columns, .. } = model {
                    columns.push(column);
                }
            } else {
                results.insert(
                    full_index_name,
                    StructModel::IndexModel {
                        database_name: Self::get_str_with_null(&row, TABLE_SCHEMA).unwrap(),
                        schema_name: String::new(),
                        table_name: Self::get_str_with_null(&row, TABLE_NAME).unwrap(),
                        index_name: Self::get_str_with_null(&row, INDEX_NAME).unwrap(),
                        index_kind: self
                            .build_index_kind(
                                row.try_get(NON_UNIQUE)?,
                                Self::get_str_with_null(&row, INDEX_NAME).unwrap().as_str(),
                            )
                            .unwrap(),
                        index_type: Self::get_str_with_null(&row, INDEX_TYPE).unwrap(),
                        comment: Self::get_str_with_null(&row, COMMENT).unwrap(),
                        tablespace: String::new(),
                        definition: String::new(),
                        columns: vec![column],
                    },
                );
            }
        }
        Ok(results)
    }

    // Check Constraint: https://dev.mysql.com/doc/refman/8.0/en/create-table-check-constraints.html
    pub async fn get_constraint(
        &mut self,
        struct_model: &Option<StructModel>,
    ) -> Result<HashMap<String, StructModel>, Error> {
        let struct_model = match struct_model {
            Some(model) => model.clone(),
            None => StructModel::ConstraintModel {
                database_name: self.db.clone(),
                schema_name: String::new(),
                table_name: String::new(),
                constraint_name: String::new(),
                constraint_type: String::new(),
                definition: String::new(),
            },
        };
        let sql = self.sql_builder(&struct_model);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);

        let mut results: HashMap<String, StructModel> = HashMap::new();

        while let Some(row) = rows.try_next().await.unwrap() {
            let (database_name, table_name, constraint_name, check_clause) = (
                Self::get_str_with_null(&row, CONSTRAINT_SCHEMA).unwrap(),
                Self::get_str_with_null(&row, TABLE_NAME).unwrap(),
                Self::get_str_with_null(&row, CONSTRAINT_NAME).unwrap(),
                Self::get_str_with_null(&row, CHECK_CLAUSE).unwrap(),
            );

            if let Some(filter) = &mut self.filter {
                if database_name.is_empty()
                    || table_name.is_empty()
                    || constraint_name.is_empty()
                    || check_clause.is_empty()
                    || filter.filter_tb(&database_name, &table_name)
                {
                    continue;
                }
            }

            results.insert(
                format!("{}.{}", database_name, table_name),
                StructModel::ConstraintModel {
                    database_name,
                    schema_name: String::new(),
                    table_name,
                    constraint_name,
                    constraint_type: "check".into(),
                    definition: check_clause,
                },
            );
        }
        Ok(results)
    }

    pub async fn fetch_with_model(
        mut self,
        struct_model: &StructModel,
    ) -> Result<Option<StructModel>, Error> {
        let model_option = Some(struct_model.to_owned());
        let result = match struct_model {
            StructModel::DatabaseModel { .. } => self.get_database(&model_option).await,
            StructModel::TableModel { .. } => self.get_table(&model_option).await,
            StructModel::IndexModel { .. } => self.get_index(&model_option).await,
            StructModel::ConstraintModel { .. } => self.get_constraint(&model_option).await,
            _ => {
                let result: HashMap<String, StructModel> = HashMap::new();
                Ok(result)
            }
        };
        match result {
            Ok(r) => {
                let result_option = r.values().next().map(|f| f.to_owned());
                Ok(result_option)
            }
            Err(e) => Err(e),
        }
    }

    fn sql_builder(&self, struct_model: &StructModel) -> String {
        let sql: String = match struct_model {
            StructModel::DatabaseModel { name } => {
                let mut s = "SELECT 
                SCHEMA_NAME, 
                DEFAULT_CHARACTER_SET_NAME, 
                DEFAULT_COLLATION_NAME 
                FROM information_schema.schemata"
                    .into();
                if !name.is_empty() {
                    s = format!("{} WHERE SCHEMA_NAME = '{}'", s, name);
                }
                s
            }

            StructModel::TableModel {
                database_name,
                table_name,
                ..
            } => {
                let mut s = format!(
                    "SELECT t.TABLE_SCHEMA,
                        t.TABLE_NAME, 
                        t.ENGINE, 
                        t.TABLE_COMMENT, 
                        c.COLUMN_NAME, 
                        c.ORDINAL_POSITION, 
                        c.COLUMN_DEFAULT, 
                        c.IS_NULLABLE, 
                        c.COLUMN_TYPE, 
                        c.COLUMN_KEY, 
                        c.EXTRA, 
                        c.COLUMN_COMMENT, 
                        c.CHARACTER_SET_NAME, 
                        c.COLLATION_NAME 
                    FROM information_schema.tables t
                    LEFT JOIN information_schema.columns c
                    ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
                    WHERE t.TABLE_SCHEMA ='{}'",
                    database_name
                );
                if !table_name.is_empty() {
                    s = format!("{} AND t.TABLE_NAME = '{}'", s, table_name);
                }
                format!(
                    "{} ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION",
                    s
                )
            }

            StructModel::IndexModel {
                database_name,
                table_name,
                index_name,
                ..
            } => {
                let mut s = format!(
                    "SELECT TABLE_SCHEMA,
                    TABLE_NAME,
                    NON_UNIQUE,
                    INDEX_NAME,
                    SEQ_IN_INDEX,
                    COLUMN_NAME,
                    INDEX_TYPE,
                    COMMENT
                FROM information_schema.statistics
                WHERE INDEX_NAME != '{}' AND TABLE_SCHEMA ='{}'",
                    PRIMARY, database_name
                );
                if !table_name.is_empty() {
                    s = format!("{} and TABLE_NAME = '{}' ", s, table_name);
                }
                if !index_name.is_empty() {
                    s = format!("{} and INDEX_NAME = '{}' ", s, index_name);
                }
                format!(
                    "{} ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX",
                    s
                )
            }

            StructModel::ConstraintModel {
                database_name,
                table_name,
                constraint_name,
                ..
            } => {
                let mut s = format!(
                    "SELECT 
                tc.CONSTRAINT_SCHEMA, 
                tc.TABLE_NAME, 
                tc.CONSTRAINT_NAME, 
                tc.CONSTRAINT_TYPE,
                cc.CHECK_CLAUSE 
                FROM information_schema.table_constraints tc 
                LEFT JOIN information_schema.check_constraints cc 
                ON tc.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA AND tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME 
                WHERE tc.CONSTRAINT_SCHEMA = '{}' and tc.CONSTRAINT_TYPE='{}' ",
                    database_name, CHECK
                );
                if !table_name.is_empty() && !constraint_name.is_empty() {
                    s = format!(
                        "{} and tc.CONSTRAINT_NAME = '{}' and tc.TABLE_NAME = '{}' ",
                        s, constraint_name, table_name
                    );
                }
                s
            }

            _ => String::new(),
        };

        sql
    }

    fn build_index_kind(&self, non_unique: i32, index_name: &str) -> Result<IndexKind, Error> {
        if index_name == PRIMARY && non_unique == 0 {
            Ok(IndexKind::PrimaryKey)
        } else if non_unique == 0 {
            Ok(IndexKind::Unique)
        } else {
            Ok(IndexKind::Index)
        }
    }

    fn get_str_with_null(row: &MySqlRow, col_name: &str) -> Result<String, Error> {
        let mut str_val = String::new();
        let str_val_option = row.get(col_name);
        if let Some(s) = str_val_option {
            str_val = s;
        }
        Ok(str_val)
    }
}
