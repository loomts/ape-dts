use std::{collections::HashSet, time::Duration};

use async_trait::async_trait;
use dt_common::{
    config::{
        extractor_config::ExtractorConfig, filter_config::FilterConfig,
        router_config::RouterConfig, sinker_config::SinkerConfig,
    },
    meta::{db_enums::DbType, db_table_model::DbTable},
};
use regex::Regex;
use sqlx::{mysql::MySqlPoolOptions, query, MySql, Pool, Row};

use crate::{
    config::precheck_config::PrecheckConfig,
    error::Error,
    meta::{check_item::CheckItem, check_result::CheckResult},
};
use futures::TryStreamExt;

use super::traits::Checker;

const MYSQL_SUPPORT_DB_VERSION_REGEX: &str = r"8\..*";

pub struct MySqlChecker {
    pub pool: Option<Pool<MySql>>,
    pub source_config: ExtractorConfig,
    pub filter_config: FilterConfig,
    pub sinker_config: SinkerConfig,
    pub router_config: RouterConfig,
    pub precheck_config: PrecheckConfig,
    pub is_source: bool,
    pub db_type_option: Option<DbType>,
}

#[async_trait]
impl Checker for MySqlChecker {
    async fn build_connection(&mut self) -> Result<CheckResult, Error> {
        let mut check_error: Option<Error> = None;
        let mut connection_url = String::from("");

        if self.is_source {
            match &self.source_config {
                ExtractorConfig::BasicConfig { url, db_type } => {
                    connection_url = String::from(url);
                    self.db_type_option = Some(db_type.clone());
                }
                _ => {}
            };
        } else {
            match &self.sinker_config {
                SinkerConfig::BasicConfig { url, db_type } => {
                    connection_url = String::from(url);
                    self.db_type_option = Some(db_type.clone());
                }
                _ => {}
            };
        }
        if !connection_url.is_empty() {
            let db_pool_result = MySqlPoolOptions::new()
                .max_connections(8)
                .acquire_timeout(Duration::from_secs(5))
                .connect(connection_url.as_str())
                .await;
            match db_pool_result {
                Ok(pool) => self.pool = Option::Some(pool),
                Err(error) => check_error = Some(Error::from(error)),
            }
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckDatabaseConnection,
            self.is_source,
            self.db_type_option.clone(),
            check_error,
        ))
    }

    // support MySQL 8.*
    async fn check_database_version(&self) -> Result<CheckResult, Error> {
        let mut check_error: Option<Error> = None;
        let mysql_pool: &Pool<MySql>;

        match &self.pool {
            Some(pool) => mysql_pool = pool,
            None => return Err(Error::from(sqlx::Error::PoolClosed)),
        }
        let sql = format!("select version()");
        println!(
            "[check_database_version] mysql query database version: {}",
            sql
        );
        let rows_result = query(&sql).fetch_all(mysql_pool).await;
        match rows_result {
            Ok(rows) => {
                if rows.len() > 0 {
                    let version: String = rows.get(0).unwrap().get("version()");
                    let re = Regex::new(MYSQL_SUPPORT_DB_VERSION_REGEX).unwrap();
                    if !re.is_match(version.as_str()) {
                        check_error = Some(Error::PreCheckError {
                            error: format!("mysql version:[{}] is invalid.", version),
                        });
                    }
                } else {
                    check_error = Some(Error::PreCheckError {
                        error: format!("found no version info with sql:[{}]", sql),
                    });
                }
            }
            Err(e) => check_error = Some(Error::from(e)),
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckDatabaseVersionSupported,
            self.is_source,
            self.db_type_option.clone(),
            check_error,
        ))
    }

    async fn check_permission(&self) -> Result<CheckResult, Error> {
        Ok(CheckResult::build(
            CheckItem::CheckAccountPermission,
            self.is_source,
        ))
    }

    async fn check_cdc_supported(&self) -> Result<CheckResult, Error> {
        let mut check_error: Option<Error> = None;
        let mysql_pool: &Pool<MySql>;

        if !self.is_source {
            // do nothing when the database is a target
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckIfDatabaseSupportCdc,
                self.is_source,
                self.db_type_option.clone(),
                check_error,
            ));
        }

        match &self.pool {
            Some(pool) => mysql_pool = pool,
            None => return Err(Error::from(sqlx::Error::PoolClosed)),
        }

        let mut errs: Vec<String> = vec![];
        // log_bin=ON, binlog_format=row, binlog_row_image=full,
        let sql = format!(
            "show variables where variable_name in ('log_bin','binlog_format','binlog_row_image')"
        );
        println!("[check_cdc_supported] mysql query cdc settings: {}", sql);
        let rows_result = query(&sql).fetch_all(mysql_pool).await;
        match rows_result {
            Ok(rows) => {
                for row in rows {
                    let variable_name: String = row.get("Variable_name");
                    let value: String = row.get("Value");
                    match variable_name.as_str() {
                        "log_bin" => {
                            if value.to_lowercase() != "on" {
                                errs.push(format!(
                                    "log_bin setting:[{}] is not 'on'.",
                                    value.to_lowercase()
                                ));
                            }
                        }
                        "binlog_row_image" => {
                            if value.to_lowercase() != "full" {
                                errs.push(format!(
                                    "binlog_row_image setting:[{}] is not 'full'",
                                    value.to_lowercase()
                                ));
                            }
                        }
                        "binlog_format" => {
                            if value.to_lowercase() != "row" {
                                errs.push(format!(
                                    "binlog_format setting:[{}] is not 'row'.",
                                    value.to_lowercase()
                                ));
                            }
                        }
                        _ => {
                            return Err(Error::PreCheckError {
                                error: "find database cdc settings meet unknown error".to_string(),
                            })
                        }
                    }
                }
            }
            Err(e) => check_error = Some(Error::from(e)),
        }

        if check_error.is_none() && errs.len() > 0 {
            check_error = Some(Error::PreCheckError {
                error: errs.join(";"),
            })
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfDatabaseSupportCdc,
            self.is_source,
            self.db_type_option.clone(),
            check_error,
        ))
    }

    async fn check_struct_existed_or_not(&self) -> Result<CheckResult, Error> {
        let mut check_error: Option<Error> = None;
        let mysql_pool: &Pool<MySql>;

        match &self.pool {
            Some(pool) => mysql_pool = pool,
            None => return Err(Error::from(sqlx::Error::PoolClosed)),
        }

        let (mut models, mut err_msgs): (Vec<DbTable>, Vec<String>) = (Vec::new(), Vec::new());
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

        if (self.is_source || !self.precheck_config.do_struct_init) && tbs.len() > 0 {
            // When a specific table to be migrated is specified and the following conditions are met, check the existence of the table
            // 1. this check is for the source database
            // 2. this check is for the sink database, and specified no structure initialization
            let (mut current_tbs, mut not_existed_tbs): (HashSet<String>, HashSet<String>) =
                (HashSet::new(), HashSet::new());

            let table_sql = format!("select t.table_schema,t.table_name,t.engine,t.table_comment,c.column_name,c.ordinal_position,c.column_default,c.is_nullable,c.column_type,c.column_key,c.extra,c.column_comment,c.character_set_name,c.collation_name
            from information_schema.tables t left join information_schema.columns c on t.table_schema = c.table_schema and t.table_name = c.table_name where t.table_schema in ({}) order by t.table_schema, t.table_name",
            all_db_names.iter().map(|x| format!("'{}'",x)).collect::<Vec<_>>().join(","));
            println!(
                "[check_struct_existed_or_not] mysql query tables sql:{}",
                table_sql
            );
            let mut rows = query(table_sql.as_str()).fetch(mysql_pool);
            while let Some(row) = rows.try_next().await.unwrap() {
                let (db, table): (String, String) =
                    (row.get("TABLE_SCHEMA"), row.get("TABLE_NAME"));
                let db_tb_name = format!("{}.{}", db, table);
                if !tbs.contains(&db_tb_name) && !dbs.contains(&db) {
                    continue;
                }
                current_tbs.insert(db_tb_name);
            }
            for tb in tbs {
                if !current_tbs.contains(&tb) {
                    not_existed_tbs.insert(tb);
                }
            }
            if not_existed_tbs.len() > 0 {
                err_msgs.push(format!(
                    "tables not existed: [{}]",
                    not_existed_tbs
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<String>>()
                        .join(";")
                ));
            }
        }

        let (mut current_dbs, mut not_existed_dbs): (HashSet<String>, HashSet<String>) =
            (HashSet::new(), HashSet::new());
        let db_sql = format!(
            "select schema_name from information_schema.SCHEMATA where schema_name in ({})",
            all_db_names
                .iter()
                .map(|x| format!("'{}'", x))
                .collect::<Vec<_>>()
                .join(",")
        );
        println!(
            "[check_struct_existed_or_not] mysql query dbs sql:{}",
            db_sql
        );
        let mut rows = query(db_sql.as_str()).fetch(mysql_pool);
        while let Some(row) = rows.try_next().await? {
            println!("{:?}", row);
            let schema_name: String = row.get("SCHEMA_NAME");
            if !dbs.contains(&schema_name) && !tb_dbs.contains(&schema_name) {
                continue;
            }
            current_dbs.insert(schema_name);
        }
        for db_name in all_db_names {
            if !current_dbs.contains(db_name) {
                not_existed_dbs.insert(db_name.clone());
            }
        }
        if not_existed_dbs.len() > 0 {
            err_msgs.push(format!(
                "databases not existed: [{}]",
                not_existed_dbs
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<String>>()
                    .join(";")
            ));
        }

        if err_msgs.len() > 0 {
            check_error = Some(Error::PreCheckError {
                error: err_msgs.join("."),
            })
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfStructExisted,
            self.is_source,
            self.db_type_option.clone(),
            check_error,
        ))
    }

    async fn check_table_structs(&self) -> Result<CheckResult, Error> {
        let mut check_error: Option<Error> = None;
        let mysql_pool: &Pool<MySql>;

        if !self.is_source {
            // do nothing when the database is a target
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckIfTableStructSupported,
                self.is_source,
                self.db_type_option.clone(),
                check_error,
            ));
        }

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

        let (mut has_pk_tables, mut has_fk_tables, mut no_pk_tables, mut err_msgs): (
            HashSet<String>,
            HashSet<String>,
            HashSet<String>,
            Vec<String>,
        ) = (HashSet::new(), HashSet::new(), HashSet::new(), Vec::new());

        let constraint_sql = format!("select table_schema,table_name,constraint_type from information_schema.table_constraints where constraint_schema in ({})", 
            all_db_names.iter().map(|x| format!("'{}'",x)).collect::<Vec<_>>().join(","));
        println!(
            "[check_table_structs] mysql check table structs sql:{}",
            constraint_sql
        );
        let mut rows = query(constraint_sql.as_str()).fetch(mysql_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let (db, table, constraint_type): (String, String, String) = (
                row.get("TABLE_SCHEMA"),
                row.get("TABLE_NAME"),
                row.get("CONSTRAINT_TYPE"),
            );
            let db_tb_name = format!("{}.{}", db, table);
            if !tbs.contains(&db_tb_name) && !dbs.contains(&db) {
                continue;
            }
            match constraint_type.as_str() {
                "PRIMARY KEY" => has_pk_tables.insert(db_tb_name),
                "FOREIGN KEY" => has_fk_tables.insert(db_tb_name),
                _ => true,
            };
        }

        let table_sql = format!("select t.table_schema,t.table_name,t.engine,t.table_comment,c.column_name,c.ordinal_position,c.column_default,c.is_nullable,c.column_type,c.column_key,c.extra,c.column_comment,c.character_set_name,c.collation_name
            from information_schema.tables t left join information_schema.columns c on t.table_schema = c.table_schema and t.table_name = c.table_name where t.table_schema in ({}) order by t.table_schema, t.table_name",
            all_db_names.iter().map(|x| format!("'{}'",x)).collect::<Vec<_>>().join(","));
        println!("[check_table_structs] mysql query tables sql:{}", table_sql);
        let mut rows = query(table_sql.as_str()).fetch(mysql_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let (db, table): (String, String) = (row.get("TABLE_SCHEMA"), row.get("TABLE_NAME"));
            let db_tb_name = format!("{}.{}", db, table);
            if !tbs.contains(&db_tb_name) && !dbs.contains(&db) {
                continue;
            }
            if !has_pk_tables.contains(&db_tb_name) {
                no_pk_tables.insert(db_tb_name);
            }
        }

        if has_fk_tables.len() > 0 {
            err_msgs.push(format!(
                "foreign keys are not supported, but these tables have foreign keys:[{}]",
                has_fk_tables
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<String>>()
                    .join(";")
            ))
        }
        if no_pk_tables.len() > 0 {
            err_msgs.push(format!(
                "primary key are needed, but these tables don't have a primary key:[{}]",
                no_pk_tables
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<String>>()
                    .join(";")
            ))
        }

        if err_msgs.len() > 0 {
            check_error = Some(Error::PreCheckError {
                error: err_msgs.join(";"),
            })
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfTableStructSupported,
            self.is_source,
            self.db_type_option.clone(),
            check_error,
        ))
    }
}
