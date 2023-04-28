use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use concurrent_queue::ConcurrentQueue;
use dt_common::{
    config::filter_config::FilterConfig,
    meta::{db_enums::DbType, db_table_model::DbTable},
};
use futures::TryStreamExt;
use sqlx::{mysql::*, query, Pool, Row};

use async_trait::async_trait;

use crate::{
    error::Error,
    meta::common::database_model::{Column, IndexColumn, IndexKind, StructModel},
    traits::StructExtrator,
    utils::queue_operator::QueueOperator,
};

pub struct MySqlStructExtractor<'a> {
    pub pool: Option<Pool<MySql>>,
    pub struct_obj_queue: &'a ConcurrentQueue<StructModel>,
    pub url: String,
    pub db_type: DbType,
    pub filter_config: FilterConfig,
    pub is_finished: Arc<AtomicBool>,
    pub is_do_check: bool,
}

#[async_trait]
// Todo:
// 1. visable, generate column, partition, compression and so on
// 2. foreign key
// 3. more model, such as view, udf, trigger
impl StructExtrator for MySqlStructExtractor<'_> {
    // fn support_db_type() {}
    // fn is_db_version_supported(_db_version: String) {}

    fn set_finished(&self) -> Result<(), Error> {
        self.is_finished.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn is_finished(&self) -> Result<bool, Error> {
        Ok(self.is_finished.load(Ordering::SeqCst))
    }

    async fn build_connection(&mut self) -> Result<(), Error> {
        let db_pool = MySqlPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&self.url)
            .await?;
        self.pool = Option::Some(db_pool);
        Ok(())
    }

    async fn get_sequence(&mut self) -> Result<Vec<StructModel>, Error> {
        Ok(vec![])
    }

    // Create Table: https://dev.mysql.com/doc/refman/8.0/en/create-table.html
    async fn get_table(&self) -> Result<Vec<StructModel>, Error> {
        let mysql_pool: &Pool<MySql>;
        match &self.pool {
            Some(pool) => mysql_pool = pool,
            None => return Err(Error::from(sqlx::Error::PoolClosed)),
        }
        let mut models: Vec<DbTable> = Vec::new();
        match &self.filter_config {
            FilterConfig::Rdb {
                do_dbs,
                ignore_dbs: _,
                do_tbs,
                ignore_tbs: _,
                do_events: _,
            } => {
                if !do_tbs.is_empty() {
                    DbTable::from_str(do_tbs, &mut models)
                } else if !do_dbs.is_empty() {
                    DbTable::from_str(do_dbs, &mut models)
                }
            }
        }

        let (dbs, tb_dbs, tbs) = DbTable::get_config_maps(&models).unwrap();
        let mut all_db_names = Vec::new();
        all_db_names.extend(&dbs);
        all_db_names.extend(&tb_dbs);

        let sql = format!("select t.table_schema,t.table_name,t.engine,t.table_comment,c.column_name,c.ordinal_position,c.column_default,c.is_nullable,c.column_type,c.column_key,c.extra,c.column_comment,c.character_set_name,c.collation_name
            from information_schema.tables t left join information_schema.columns c on t.table_schema = c.table_schema and t.table_name = c.table_name where t.table_schema in ({}) order by t.table_schema, t.table_name",
            all_db_names.iter().map(|x| format!("'{}'",x)).collect::<Vec<_>>().join(","));
        println!("get_table_sql:{}", sql);
        let mut rows = query(sql.as_str()).fetch(mysql_pool);
        let mut db_tb_map: HashMap<String, StructModel> = HashMap::new();
        while let Some(row) = rows.try_next().await? {
            let (db, table): (String, String) = (
                MySqlStructExtractor::get_str_with_null(&row, "TABLE_SCHEMA").unwrap(),
                MySqlStructExtractor::get_str_with_null(&row, "TABLE_NAME").unwrap(),
            );
            let db_tb_name = format!("{}.{}", db, table);
            if !tbs.contains(&db_tb_name) && !dbs.contains(&db) {
                println!("db:{},table:{} should not migrate", db, table);
                continue;
            }
            if db_tb_map.contains_key(&db_tb_name) {
                match db_tb_map.get_mut(&db_tb_name).unwrap() {
                    StructModel::TableModel {
                        database_name: _,
                        schema_name: _,
                        table_name: _,
                        engine_name: _,
                        table_comment: _,
                        columns,
                    } => columns.push(Column {
                        column_name: MySqlStructExtractor::get_str_with_null(&row, "COLUMN_NAME")
                            .unwrap(),
                        order_position: row.try_get("ORDINAL_POSITION")?,
                        default_value: row.get("COLUMN_DEFAULT"),
                        is_nullable: MySqlStructExtractor::get_str_with_null(&row, "IS_NULLABLE")
                            .unwrap(),
                        column_type: MySqlStructExtractor::get_str_with_null(&row, "COLUMN_TYPE")
                            .unwrap(),
                        column_key: MySqlStructExtractor::get_str_with_null(&row, "COLUMN_KEY")
                            .unwrap(),
                        extra: MySqlStructExtractor::get_str_with_null(&row, "EXTRA").unwrap(),
                        column_comment: MySqlStructExtractor::get_str_with_null(
                            &row,
                            "COLUMN_COMMENT",
                        )
                        .unwrap(),
                        character_set: MySqlStructExtractor::get_str_with_null(
                            &row,
                            "CHARACTER_SET_NAME",
                        )
                        .unwrap(),
                        collation: MySqlStructExtractor::get_str_with_null(&row, "COLLATION_NAME")
                            .unwrap(),
                        generated: None,
                    }),
                    _ => {}
                }
            } else {
                db_tb_map.insert(
                    db_tb_name,
                    StructModel::TableModel {
                        database_name: MySqlStructExtractor::get_str_with_null(
                            &row,
                            "TABLE_SCHEMA",
                        )
                        .unwrap(),
                        schema_name: String::from(""),
                        table_name: MySqlStructExtractor::get_str_with_null(&row, "TABLE_NAME")
                            .unwrap(),
                        engine_name: MySqlStructExtractor::get_str_with_null(&row, "ENGINE")
                            .unwrap(),
                        table_comment: MySqlStructExtractor::get_str_with_null(
                            &row,
                            "TABLE_COMMENT",
                        )
                        .unwrap(),
                        columns: vec![Column {
                            column_name: MySqlStructExtractor::get_str_with_null(
                                &row,
                                "COLUMN_NAME",
                            )
                            .unwrap(),
                            order_position: row.try_get("ORDINAL_POSITION")?,
                            default_value: row.get("COLUMN_DEFAULT"),
                            is_nullable: MySqlStructExtractor::get_str_with_null(
                                &row,
                                "IS_NULLABLE",
                            )
                            .unwrap(),
                            column_type: MySqlStructExtractor::get_str_with_null(
                                &row,
                                "COLUMN_TYPE",
                            )
                            .unwrap(),
                            column_key: MySqlStructExtractor::get_str_with_null(&row, "COLUMN_KEY")
                                .unwrap(),
                            extra: MySqlStructExtractor::get_str_with_null(&row, "EXTRA").unwrap(),
                            column_comment: MySqlStructExtractor::get_str_with_null(
                                &row,
                                "COLUMN_COMMENT",
                            )
                            .unwrap(),
                            character_set: MySqlStructExtractor::get_str_with_null(
                                &row,
                                "CHARACTER_SET_NAME",
                            )
                            .unwrap(),
                            collation: MySqlStructExtractor::get_str_with_null(
                                &row,
                                "COLLATION_NAME",
                            )
                            .unwrap(),
                            generated: None,
                        }],
                    },
                );
            }
        }
        let mut result_vec: Vec<StructModel> = vec![];
        if db_tb_map.len() > 0 {
            for (_, model) in &db_tb_map {
                if self.is_do_check {
                    result_vec.push(model.clone());
                } else {
                    let _ = QueueOperator::push_to_queue(&self.struct_obj_queue, model.clone(), 1)
                        .await;
                }
            }
        }
        println!("get table finished");
        Ok(result_vec)
    }

    // Create Index: https://dev.mysql.com/doc/refman/8.0/en/create-index.html
    async fn get_index(&self) -> Result<Vec<StructModel>, Error> {
        let mysql_pool: &Pool<MySql>;
        match &self.pool {
            Some(pool) => mysql_pool = pool,
            None => return Err(Error::from(sqlx::Error::PoolClosed)),
        }
        let mut models: Vec<DbTable> = Vec::new();
        match &self.filter_config {
            FilterConfig::Rdb {
                do_dbs,
                ignore_dbs: _,
                do_tbs,
                ignore_tbs: _,
                do_events: _,
            } => {
                if !do_tbs.is_empty() {
                    DbTable::from_str(do_tbs, &mut models)
                } else if !do_dbs.is_empty() {
                    DbTable::from_str(do_dbs, &mut models)
                }
            }
        }
        let (dbs, tb_dbs, tbs) = DbTable::get_config_maps(&models).unwrap();
        let mut all_db_names = Vec::new();
        all_db_names.extend(&dbs);
        all_db_names.extend(&tb_dbs);

        let sql = format!("select table_schema,table_name,non_unique,index_name, seq_in_index,column_name,index_type,is_visible,comment from information_schema.statistics 
         where index_name != 'PRIMARY' and table_schema in ({}) order by table_schema, table_name, index_name", 
            all_db_names.iter().map(|x| format!("'{}'",x)).collect::<Vec<_>>().join(","));
        println!("mysql get_index_sql:{}", sql);
        let mut rows = query(sql.as_str()).fetch(mysql_pool);

        let mut index_map: HashMap<String, StructModel> = HashMap::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let (db, table, index_name): (String, String, String) = (
                MySqlStructExtractor::get_str_with_null(&row, "TABLE_SCHEMA").unwrap(),
                MySqlStructExtractor::get_str_with_null(&row, "TABLE_NAME").unwrap(),
                MySqlStructExtractor::get_str_with_null(&row, "INDEX_NAME").unwrap(),
            );
            let db_tb_name = format!("{}.{}", db, table);
            if !tbs.contains(&db_tb_name) && !dbs.contains(&db) {
                println!(
                    "db:{},table:{},index:{} should not migrate",
                    db, table, index_name
                );
                continue;
            }
            let index_full_name = format!("{}.{}.{}", db, table, index_name);
            if index_map.contains_key(index_name.as_str()) {
                match index_map.get_mut(index_name.as_str()).unwrap() {
                    StructModel::IndexModel {
                        database_name: _,
                        schema_name: _,
                        table_name: _,
                        index_name: _,
                        index_kind: _,
                        index_type: _,
                        comment: _,
                        tablespace: _,
                        definition: _,
                        columns,
                    } => columns.push(IndexColumn {
                        column_name: MySqlStructExtractor::get_str_with_null(&row, "COLUMN_NAME")
                            .unwrap(),
                        seq_in_index: row.try_get("SEQ_IN_INDEX")?,
                    }),
                    _ => {}
                }
            } else {
                index_map.insert(
                    index_full_name,
                    StructModel::IndexModel {
                        database_name: MySqlStructExtractor::get_str_with_null(
                            &row,
                            "TABLE_SCHEMA",
                        )
                        .unwrap(),
                        schema_name: String::from(""),
                        table_name: MySqlStructExtractor::get_str_with_null(&row, "TABLE_NAME")
                            .unwrap(),
                        index_name: MySqlStructExtractor::get_str_with_null(&row, "INDEX_NAME")
                            .unwrap(),
                        index_kind: self
                            .build_index_kind(
                                row.try_get("NON_UNIQUE")?,
                                MySqlStructExtractor::get_str_with_null(&row, "INDEX_NAME")
                                    .unwrap()
                                    .as_str(),
                            )
                            .unwrap(),
                        index_type: MySqlStructExtractor::get_str_with_null(&row, "INDEX_TYPE")
                            .unwrap(),
                        comment: MySqlStructExtractor::get_str_with_null(&row, "COMMENT").unwrap(),
                        tablespace: String::from(""),
                        definition: String::from(""),
                        columns: vec![IndexColumn {
                            column_name: MySqlStructExtractor::get_str_with_null(
                                &row,
                                "COLUMN_NAME",
                            )
                            .unwrap(),
                            seq_in_index: row.try_get("SEQ_IN_INDEX")?,
                        }],
                    },
                );
            }
        }
        let mut result_vec: Vec<StructModel> = vec![];
        if index_map.len() > 0 {
            for (_, model) in &index_map {
                if self.is_do_check {
                    result_vec.push(model.clone());
                } else {
                    let _ = QueueOperator::push_to_queue(&self.struct_obj_queue, model.clone(), 1)
                        .await;
                }
            }
        }
        println!("get index finished");
        Ok(vec![])
    }

    async fn get_constraint(&self) -> Result<Vec<StructModel>, Error> {
        // Todo:
        Ok(vec![])
    }

    async fn get_comment(&self) -> Result<Vec<StructModel>, Error> {
        // do nothing here, comment is builded when Table or Column create
        Ok(vec![])
    }
}

impl MySqlStructExtractor<'_> {
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
