use std::{collections::HashMap, str::FromStr};

use anyhow::bail;
use dt_common::meta::{
    mysql::mysql_meta_manager::MysqlMetaManager,
    struct_meta::{
        statement::{
            mysql_create_database_statement::MysqlCreateDatabaseStatement,
            mysql_create_table_statement::MysqlCreateTableStatement,
        },
        structure::{
            column::Column,
            constraint::{Constraint, ConstraintType},
            database::Database,
            index::{Index, IndexColumn, IndexKind, IndexType},
            table::Table,
        },
    },
};
use dt_common::{config::config_enums::DbType, error::Error, rdb_filter::RdbFilter};
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
const REFERENCED_TABLE_NAME: &str = "REFERENCED_TABLE_NAME";
const REFERENCED_COLUMN_NAME: &str = "REFERENCED_COLUMN_NAME";

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

const CHARACTER_SET_NAME: &str = "CHARACTER_SET_NAME";
const COLLATION_NAME: &str = "COLLATION_NAME";

const CONSTRAINT_SCHEMA: &str = "CONSTRAINT_SCHEMA";
const CONSTRAINT_NAME: &str = "CONSTRAINT_NAME";
const CHECK_CLAUSE: &str = "CHECK_CLAUSE";

impl MysqlStructFetcher {
    pub async fn get_create_database_statement(
        &mut self,
    ) -> anyhow::Result<MysqlCreateDatabaseStatement> {
        let database = self.get_database().await?;
        Ok(MysqlCreateDatabaseStatement { database })
    }

    pub async fn get_create_table_statements(
        &mut self,
        tb: &str,
    ) -> anyhow::Result<Vec<MysqlCreateTableStatement>> {
        let mut results = Vec::new();

        let tables = self.get_tables(tb).await.unwrap();
        let mut indexes = self.get_indexes(tb).await.unwrap();
        let mut check_constraints = self.get_check_constraints(tb).await.unwrap();
        let mut foreign_key_constraints = self.get_foreign_key_constraints(tb).await.unwrap();

        for (table_name, table) in tables {
            let mut constraints = self.get_result(&mut check_constraints, &table_name);
            constraints
                .extend_from_slice(&self.get_result(&mut foreign_key_constraints, &table_name));
            let statement = MysqlCreateTableStatement {
                table,
                constraints,
                indexes: self.get_result(&mut indexes, &table_name),
            };
            results.push(statement);
        }
        Ok(results)
    }

    // Create Database: https://dev.mysql.com/doc/refman/8.0/en/create-database.html
    async fn get_database(&mut self) -> anyhow::Result<Database> {
        let sql = format!(
            "SELECT 
            SCHEMA_NAME, 
            DEFAULT_CHARACTER_SET_NAME, 
            DEFAULT_COLLATION_NAME 
            FROM information_schema.schemata
            WHERE SCHEMA_NAME = '{}'",
            self.db
        );

        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        if let Some(row) = rows.try_next().await.unwrap() {
            let schema_name = Self::get_str_with_null(&row, SCHEMA_NAME).unwrap();
            let default_character_set_name =
                Self::get_str_with_null(&row, "DEFAULT_CHARACTER_SET_NAME").unwrap();
            let default_collation_name =
                Self::get_str_with_null(&row, "DEFAULT_COLLATION_NAME").unwrap();
            let database = Database {
                name: schema_name,
                default_character_set_name,
                default_collation_name,
            };
            return Ok(database);
        }

        bail! {Error::StructError(format!("db: {} not found", self.db))}
    }

    async fn get_tables(&mut self, tb: &str) -> anyhow::Result<HashMap<String, Table>> {
        let mut results: HashMap<String, Table> = HashMap::new();

        // Create Table: https://dev.mysql.com/doc/refman/8.0/en/create-table.html
        let tb_filter = if !tb.is_empty() {
            format!("AND t.TABLE_NAME = '{}'", tb)
        } else {
            String::new()
        };

        // BASE TABLE for a table, VIEW for a view, or SYSTEM VIEW for an INFORMATION_SCHEMA table.
        // refer: https://dev.mysql.com/doc/refman/8.0/en/information-schema-tables-table.html
        let sql = format!(
            "SELECT t.TABLE_SCHEMA,
                t.TABLE_NAME, 
                t.ENGINE, 
                t.TABLE_COMMENT, 
                t.TABLE_COLLATION,
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
            WHERE t.TABLE_SCHEMA ='{}' {}
            AND t.TABLE_TYPE = 'BASE TABLE' 
            ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION",
            self.db, tb_filter
        );

        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
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

            let engine_name = Self::get_str_with_null(&row, ENGINE).unwrap();
            let table_comment = Self::get_str_with_null(&row, TABLE_COMMENT).unwrap();
            let column = Column {
                column_name: Self::get_str_with_null(&row, COLUMN_NAME).unwrap(),
                ordinal_position: row.try_get(ORDINAL_POSITION).unwrap(),
                column_default: row.get(COLUMN_DEFAULT),
                is_nullable: Self::get_str_with_null(&row, IS_NULLABLE).unwrap(),
                column_type: Self::get_str_with_null(&row, COLUMN_TYPE).unwrap(),
                column_key: Self::get_str_with_null(&row, COLUMN_KEY).unwrap(),
                extra: Self::get_str_with_null(&row, EXTRA).unwrap(),
                column_comment: Self::get_str_with_null(&row, COLUMN_COMMENT).unwrap(),
                character_set_name: Self::get_str_with_null(&row, CHARACTER_SET_NAME).unwrap(),
                collation_name: Self::get_str_with_null(&row, COLLATION_NAME).unwrap(),
                generated: None,
            };

            if let Some(table) = results.get_mut(&tb) {
                table.columns.push(column);
            } else {
                let table_collation = Self::get_str_with_null(&row, "TABLE_COLLATION").unwrap();
                let charset = Self::get_charset_by_collation(&table_collation);
                results.insert(
                    tb.clone(),
                    Table {
                        database_name: db.clone(),
                        schema_name: String::new(),
                        table_name: tb,
                        engine_name,
                        table_comment,
                        character_set: charset,
                        table_collation,
                        columns: vec![column],
                    },
                );
            }
        }

        Ok(results)
    }

    async fn get_indexes(&mut self, tb: &str) -> anyhow::Result<HashMap<String, Vec<Index>>> {
        let mut index_map: HashMap<(String, String), Index> = HashMap::new();

        // Create Index: https://dev.mysql.com/doc/refman/8.0/en/create-index.html
        let tb_filter = if !tb.is_empty() {
            format!("AND TABLE_NAME = '{}'", tb)
        } else {
            String::new()
        };

        let sql = format!(
            "SELECT TABLE_SCHEMA,
                TABLE_NAME,
                NON_UNIQUE,
                INDEX_NAME,
                SEQ_IN_INDEX,
                COLUMN_NAME,
                INDEX_TYPE,
                COMMENT
            FROM information_schema.statistics
            WHERE INDEX_NAME != '{}' AND TABLE_SCHEMA ='{}' {}
            ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX",
            PRIMARY, self.db, tb_filter
        );

        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let (table_name, index_name) = (
                Self::get_str_with_null(&row, TABLE_NAME).unwrap(),
                Self::get_str_with_null(&row, INDEX_NAME).unwrap(),
            );

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

            let key = (table_name.clone(), index_name.clone());
            if let Some(index) = index_map.get_mut(&key) {
                index.columns.push(column);
            } else {
                let non_unique = row.try_get(NON_UNIQUE).unwrap();
                let index_type_str = Self::get_str_with_null(&row, INDEX_TYPE).unwrap();
                let index_type = IndexType::from_str(&index_type_str).unwrap();
                let index_kind = Self::get_index_kind(non_unique, &index_type);
                let index = Index {
                    database_name: Self::get_str_with_null(&row, TABLE_SCHEMA).unwrap(),
                    table_name,
                    index_name,
                    index_kind,
                    index_type,
                    comment: Self::get_str_with_null(&row, COMMENT).unwrap(),
                    columns: vec![column],
                    ..Default::default()
                };
                index_map.insert(key, index);
            }
        }

        let mut results: HashMap<String, Vec<Index>> = HashMap::new();
        for ((tb, _index_name), index) in index_map {
            self.push_to_results(&mut results, &tb, index);
        }

        Ok(results)
    }

    async fn get_check_constraints(
        &mut self,
        tb: &str,
    ) -> anyhow::Result<HashMap<String, Vec<Constraint>>> {
        let mut results: HashMap<String, Vec<Constraint>> = HashMap::new();
        // mysql 5.7 does not support check constraints
        if self.meta_manager.version.starts_with("5.") {
            return Ok(results);
        }

        // Check Constraint: https://dev.mysql.com/doc/refman/8.0/en/create-table-check-constraints.html
        let tb_filter = if !tb.is_empty() {
            format!("AND tc.TABLE_NAME = '{}'", tb)
        } else {
            String::new()
        };

        let constraint_type_str = ConstraintType::Check.to_str(DbType::Mysql);
        let sql = format!(
            "SELECT
                tc.CONSTRAINT_SCHEMA, 
                tc.TABLE_NAME, 
                tc.CONSTRAINT_NAME, 
                tc.CONSTRAINT_TYPE,
                cc.CHECK_CLAUSE 
            FROM information_schema.table_constraints tc 
            LEFT JOIN information_schema.check_constraints cc 
            ON tc.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA AND tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME 
            WHERE tc.CONSTRAINT_SCHEMA = '{}' {} 
            AND tc.CONSTRAINT_TYPE='{}' ", 
            self.db, tb_filter, constraint_type_str
        );

        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let database_name = Self::get_str_with_null(&row, CONSTRAINT_SCHEMA).unwrap();
            let table_name = Self::get_str_with_null(&row, TABLE_NAME).unwrap();
            let constraint_name = Self::get_str_with_null(&row, CONSTRAINT_NAME).unwrap();
            let check_clause = Self::get_str_with_null(&row, CHECK_CLAUSE).unwrap();
            let definition = self.unescape(check_clause).await.unwrap();
            let constraint = Constraint {
                database_name,
                schema_name: String::new(),
                table_name: table_name.clone(),
                constraint_name,
                constraint_type: ConstraintType::Check,
                definition,
            };
            self.push_to_results(&mut results, &table_name, constraint);
        }

        Ok(results)
    }

    async fn get_foreign_key_constraints(
        &mut self,
        tb: &str,
    ) -> anyhow::Result<HashMap<String, Vec<Constraint>>> {
        let mut results: HashMap<String, Vec<Constraint>> = HashMap::new();

        // Check Constraint: https://dev.mysql.com/doc/refman/8.0/en/create-table-check-constraints.html
        let tb_filter = if !tb.is_empty() {
            format!("AND kcu.TABLE_NAME = '{}'", tb)
        } else {
            String::new()
        };

        let constraint_type_str = ConstraintType::Foregin.to_str(DbType::Mysql);
        let sql = format!(
            "SELECT
                kcu.CONSTRAINT_NAME,
                kcu.CONSTRAINT_SCHEMA,
                kcu.TABLE_NAME,
                kcu.COLUMN_NAME,
                kcu.REFERENCED_TABLE_SCHEMA,
                kcu.REFERENCED_TABLE_NAME,
                kcu.REFERENCED_COLUMN_NAME
            FROM
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND kcu.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA
            WHERE
                kcu.CONSTRAINT_SCHEMA = '{}'
                AND kcu.REFERENCED_TABLE_SCHEMA = '{}' {}
                AND tc.CONSTRAINT_TYPE = '{}'",
            self.db, self.db, tb_filter, constraint_type_str,
        );

        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let database_name = Self::get_str_with_null(&row, CONSTRAINT_SCHEMA).unwrap();
            let constraint_name = Self::get_str_with_null(&row, CONSTRAINT_NAME).unwrap();
            let table_name = Self::get_str_with_null(&row, TABLE_NAME).unwrap();
            let column_name = Self::get_str_with_null(&row, COLUMN_NAME).unwrap();
            let referenced_table_name =
                Self::get_str_with_null(&row, REFERENCED_TABLE_NAME).unwrap();
            let referenced_column_name =
                Self::get_str_with_null(&row, REFERENCED_COLUMN_NAME).unwrap();
            let definition = format!(
                "(`{}`) REFERENCES `{}`.`{}`(`{}`)",
                column_name, database_name, referenced_table_name, referenced_column_name
            );
            let constraint = Constraint {
                database_name,
                schema_name: String::new(),
                table_name: table_name.clone(),
                constraint_name,
                constraint_type: ConstraintType::Foregin,
                definition,
            };
            self.push_to_results(&mut results, &table_name, constraint);
        }
        Ok(results)
    }

    fn get_str_with_null(row: &MySqlRow, col_name: &str) -> anyhow::Result<String> {
        let mut str_val = String::new();
        let str_val_option = row.get(col_name);
        if let Some(s) = str_val_option {
            str_val = s;
        }
        Ok(str_val)
    }

    fn filter_tb(&mut self, tb: &str) -> bool {
        if let Some(filter) = &mut self.filter {
            return filter.filter_tb(&self.db, tb);
        }
        false
    }

    fn get_index_kind(non_unique: i32, index_type: &IndexType) -> IndexKind {
        if non_unique == 0 {
            IndexKind::Unique
        } else {
            match index_type {
                IndexType::FullText => IndexKind::FullText,
                IndexType::Spatial => IndexKind::Spatial,
                _ => IndexKind::Unknown,
            }
        }
    }

    fn get_charset_by_collation(collation: &str) -> String {
        // show all collation names by:
        // SELECT COLLATION_NAME FROM INFORMATION_SCHEMA.COLLATIONS;
        // latin1_german2_ci, utf8mb4_nb_0900_as_cs
        let tokens: Vec<&str> = collation.split('_').collect();
        if !tokens.is_empty() {
            tokens[0].to_string()
        } else {
            String::new()
        }
    }

    async fn unescape(&self, text: String) -> anyhow::Result<String> {
        // use mysql's native select 'xx' to remove the escape characters stored in the string by mysql
        if text.is_empty() {
            return Ok(text);
        }

        let sql = format!("select '{}' as result", text);
        match sqlx::query(&sql).fetch_all(&self.conn_pool).await {
            Ok(rows) => {
                if !rows.is_empty() {
                    let result: String = rows.first().unwrap().get("result");
                    return Ok(result);
                }
            }
            Err(error) => {
                bail! {Error::SqlxError(error)}
            }
        }
        Ok(text)
    }

    fn push_to_results<T>(
        &mut self,
        results: &mut HashMap<String, Vec<T>>,
        table_name: &str,
        item: T,
    ) {
        if self.filter_tb(table_name) {
            return;
        }

        if let Some(exists) = results.get_mut(table_name) {
            exists.push(item);
        } else {
            results.insert(table_name.into(), vec![item]);
        }
    }

    fn get_result<T>(&self, results: &mut HashMap<String, Vec<T>>, table_name: &str) -> Vec<T> {
        results.remove(table_name).unwrap_or_default()
    }
}
