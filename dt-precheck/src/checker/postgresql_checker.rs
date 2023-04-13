use std::{collections::HashSet, time::Duration};

use async_trait::async_trait;
use dt_common::{
    config::{
        extractor_config::ExtractorConfig, filter_config::FilterConfig,
        router_config::RouterConfig, sinker_config::SinkerConfig,
    },
    meta::{db_enums::DbType, db_table_model::DbTable, postgresql::pg_enums::ConstraintTypeEnum},
    utils::config_url_util::ConfigUrlUtil,
};
use sqlx::{postgres::PgPoolOptions, query, Pool, Postgres, Row};

use crate::{
    config::precheck_config::PrecheckConfig, error::Error, meta::check_item::CheckItem,
    meta::check_result::CheckResult,
};
use futures::TryStreamExt;

use super::traits::Checker;

const PG_SUPPORT_DB_VERSION_NUM_MIN: i32 = 140000;
const PG_SUPPORT_DB_VERSION_NUM_MAX: i32 = 149999;

pub struct PostgresqlChecker {
    pub pool: Option<Pool<Postgres>>,
    pub source_config: ExtractorConfig,
    pub filter_config: FilterConfig,
    pub sinker_config: SinkerConfig,
    pub router_config: RouterConfig,
    pub precheck_config: PrecheckConfig,
    pub is_source: bool,
    pub db_type_option: Option<DbType>,
}

#[async_trait]
impl Checker for PostgresqlChecker {
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
            let db_pool_result = PgPoolOptions::new()
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

    // Supported PostgreSQL 14.*
    async fn check_database_version(&self) -> Result<CheckResult, Error> {
        let mut check_error: Option<Error> = None;
        let pg_pool: &Pool<Postgres>;

        match &self.pool {
            Some(pool) => pg_pool = pool,
            None => return Err(Error::from(sqlx::Error::PoolClosed)),
        }
        let sql = format!("SELECT current_setting('server_version_num')::integer");
        println!("pg query database version: {}", sql);
        let rows_result = query(&sql).fetch_all(pg_pool).await;
        match rows_result {
            Ok(rows) => {
                if rows.len() > 0 {
                    let current_version: i32 = rows.get(0).unwrap().get("current_setting");
                    // Supported 14.*, the version_num with Pg14.7 is '140007'
                    if current_version < PG_SUPPORT_DB_VERSION_NUM_MIN
                        || current_version > PG_SUPPORT_DB_VERSION_NUM_MAX
                    {
                        check_error = Some(Error::PreCheckError {
                            error: format!("version:{} is not supported yet", current_version),
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
        let check_error: Option<Error> = None;
        let super_result = self.is_super_user().await;

        match super_result {
            Ok(is_super) => {
                if is_super {
                    // super user dose not neet to do validate
                    println!("the database account is a super user");
                    return Ok(CheckResult::build(CheckItem::CheckAccountPermission));
                }
            }
            Err(e) => return Err(e),
        }
        // Todo: dml、ddl, wal log、producer、slot. role and role inherit
        if self.is_source {
            // check select permission
            if self.precheck_config.do_cdc {
                // check cdc permission
            }
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckAccountPermission,
            self.is_source,
            self.db_type_option.clone(),
            check_error,
        ))
    }

    async fn check_cdc_supported(&self) -> Result<CheckResult, Error> {
        let mut check_error: Option<Error> = None;
        let pg_pool: &Pool<Postgres>;

        if !self.is_source {
            // do nothing when the database is target
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckIfDatabaseSupportCdc,
                self.is_source,
                self.db_type_option.clone(),
                check_error,
            ));
        }

        match &self.pool {
            Some(pool) => pg_pool = pool,
            None => return Err(Error::from(sqlx::Error::PoolClosed)),
        }
        // check the cdc settings
        let setting_sql = format!(
            "SELECT name,setting FROM pg_settings WHERE name in ('wal_level','max_wal_senders','max_replication_slots')"
        );
        println!("pg query cdc settings: {}", setting_sql);
        let settings = query(&setting_sql).fetch_all(pg_pool).await;
        let (
            mut wal_level,
            mut max_wel_senders,
            mut max_replication_slots,
            mut max_replication_slots_i32,
            mut err_msgs,
        ): (String, String, String, i32, Vec<String>) = (
            String::from(""),
            String::from(""),
            String::from(""),
            0,
            vec![],
        );
        let max_wal_senders_i32: i32;
        match settings {
            Ok(rows) => {
                if rows.len() > 0 {
                    for row in rows {
                        let row_name: String = row.get("name");
                        match row_name.as_str() {
                            "wal_level" => wal_level = row.get("setting"),
                            "max_replication_slots" => max_replication_slots = row.get("setting"),
                            "max_wal_senders" => max_wel_senders = row.get("setting"),
                            _ => {}
                        }
                    }
                    // wal_level=logic,max_replication_slots>=1, max_wal_senders>=1.
                    if wal_level.to_lowercase() != "logical" {
                        err_msgs.push(format!(
                            "wal_level should not be '{}', need to be 'logical'.",
                            wal_level
                        ))
                    }
                    max_replication_slots_i32 = max_replication_slots.parse().unwrap();
                    if max_replication_slots_i32 < 1 {
                        err_msgs.push(format!(
                            "max_replication_slots needs to be greater than 0. current is '{}'",
                            max_replication_slots
                        ))
                    }
                    max_wal_senders_i32 = max_wel_senders.parse().unwrap();
                    if max_wal_senders_i32 < 1 {
                        err_msgs.push(format!(
                            "max_wel_senders needs to be greater than 0, current is '{}'",
                            max_wel_senders
                        ))
                    }
                    if err_msgs.len() > 0 {
                        check_error = Some(Error::PreCheckError {
                            error: err_msgs.join(";"),
                        });
                    }
                } else {
                    check_error = Some(Error::PreCheckError {
                        error: format!("found no pg_settings with sql:[{}]", setting_sql),
                    });
                }
            }
            Err(e) => check_error = Some(Error::from(e)),
        }
        if check_error.is_none() {
            // check the slot count is less than max_replication_slots or not
            let slot_query = format!("select slot_name from pg_catalog.pg_replication_slots");
            println!("pg query slot count: {}", setting_sql);
            let current_slots = query(&slot_query).fetch_all(pg_pool).await;
            match current_slots {
                Ok(rows) => {
                    if max_replication_slots_i32 == (rows.len() as i32) {
                        check_error = Some(Error::PreCheckError { error: format!("the current number of slots:[{}] has reached max_replication_slots, and new slots cannot be created", max_replication_slots) });
                    }
                }
                Err(e) => check_error = Some(Error::from(e)),
            }
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfDatabaseSupportCdc,
            self.is_source,
            self.db_type_option.clone(),
            check_error,
        ))
    }

    async fn check_table_structs(&self) -> Result<CheckResult, Error> {
        // all tables have a pk, and have no fk
        let mut check_error: Option<Error> = None;
        let pg_pool: &Pool<Postgres>;

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
            Some(pool) => pg_pool = pool,
            None => return Err(Error::from(sqlx::Error::PoolClosed)),
        }
        let (mut db_tables, mut err_msgs): (Vec<DbTable>, Vec<String>) = (Vec::new(), Vec::new());
        match &self.filter_config {
            FilterConfig::Rdb {
                do_dbs,
                ignore_dbs: _,
                do_tbs,
                ignore_tbs: _,
                do_events: _,
            } => {
                if !do_tbs.is_empty() {
                    DbTable::from_str(do_tbs, &mut db_tables)
                } else if !do_dbs.is_empty() {
                    DbTable::from_str(do_dbs, &mut db_tables)
                }
            }
        }
        let (schemas, tb_schemas, tbs) = DbTable::get_config_maps(&db_tables).unwrap();
        let mut all_schemas = Vec::new();
        all_schemas.extend(&schemas);
        all_schemas.extend(&tb_schemas);
        if all_schemas.len() <= 0 {
            println!("found no schema need to do migrate, very strange");
            return Err(Error::PreCheckError {
                error: String::from("found no schema need to do migrate"),
            });
        }

        let (mut current_tables, mut has_pk_tables, mut has_fk_tables): (
            HashSet<String>,
            HashSet<String>,
            HashSet<String>,
        ) = (HashSet::new(), HashSet::new(), HashSet::new());

        let table_sql = format!("SELECT c.table_schema,c.table_name,c.column_name, c.data_type, c.udt_name, c.character_maximum_length, c.is_nullable, c.column_default, c.numeric_precision, c.numeric_scale, c.is_identity, c.identity_generation,c.ordinal_position 
        FROM information_schema.columns c where table_schema in ({}) ORDER BY table_schema,table_name", all_schemas.iter().map(|x| format!("'{}'", x)).collect::<Vec<String>>().join(","));
        println!("pg query table sql: {}", table_sql);
        let mut rows = query(&table_sql).fetch(pg_pool);
        while let Some(row) = rows.try_next().await? {
            let (schema_name, table_name): (String, String) =
                (row.get("table_schema"), row.get("table_name"));
            if schema_name.is_empty() && table_name.is_empty() {
                continue;
            }
            let schema_table_name = format!("{}.{}", schema_name, table_name);
            if !tbs.contains(&schema_table_name) && !schemas.contains(&schema_name) {
                continue;
            }
            current_tables.insert(schema_table_name);
        }

        let constraint_sql = format!("SELECT nsp.nspname, rel.relname, con.conname as constraint_name, con.contype as constraint_type,pg_get_constraintdef(con.oid) as constraint_definition
        FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace
        WHERE nsp.nspname in ({}) order by nsp.nspname,rel.relname", all_schemas.iter().map(|x| format!("'{}'", x)).collect::<Vec<String>>().join(","));
        println!("pg query constraint sql: {}", constraint_sql);
        let mut rows = query(&constraint_sql).fetch(pg_pool);
        while let Some(row) = rows.try_next().await? {
            let (schema_name, table_name): (String, String) =
                (row.get("nspname"), row.get("relname"));
            if schema_name.is_empty() && table_name.is_empty() {
                continue;
            }
            let schema_table_name = format!("{}.{}", schema_name, table_name);
            if !tbs.contains(&schema_table_name) && !schemas.contains(&schema_name) {
                continue;
            }
            let constrant_type: i8 = row.get("constraint_type");
            if constrant_type.to_string() == ConstraintTypeEnum::Primary.to_charval().unwrap() {
                has_pk_tables.insert(schema_table_name);
            } else if constrant_type.to_string()
                == ConstraintTypeEnum::Foregin.to_charval().unwrap()
            {
                has_fk_tables.insert(schema_table_name);
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
            ));
        }
        let mut no_pk_tables: HashSet<String> = HashSet::new();
        for current_table in current_tables {
            if !has_pk_tables.contains(&current_table) {
                no_pk_tables.insert(current_table);
            }
        }
        if no_pk_tables.len() > 0 {
            err_msgs.push(format!(
                "primary key are needed, but these tables don't have a primary key:[{}]",
                no_pk_tables
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<String>>()
                    .join(";")
            ));
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

impl PostgresqlChecker {
    async fn is_super_user(&self) -> Result<bool, Error> {
        let pg_pool: &Pool<Postgres>;
        match &self.pool {
            Some(pool) => pg_pool = pool,
            None => return Err(Error::from(sqlx::Error::PoolClosed)),
        }
        let user_option;
        if self.is_source {
            match &self.source_config {
                ExtractorConfig::BasicConfig { url, db_type: _ } => {
                    user_option = ConfigUrlUtil::get_username(String::from(url))
                }
                _ => user_option = None,
            }
        } else {
            match &self.sinker_config {
                SinkerConfig::BasicConfig { url, db_type: _ } => {
                    user_option = ConfigUrlUtil::get_username(String::from(url))
                }
                _ => user_option = None,
            }
        }
        if user_option.is_none() {
            return Err(Error::PreCheckError {
                error: String::from("username in config is invalid"),
            });
        }
        let user_str = user_option.unwrap();
        let sql = format!(
            "select rolsuper from pg_catalog.pg_roles where rolname = '{}'::varchar",
            user_str
        );
        println!("pg query is superuser: {}", sql);
        let rows_result = query(&sql).fetch_all(pg_pool).await;
        match rows_result {
            Ok(rows) => {
                if rows.len() <= 0 {
                    return Err(Error::PreCheckError {
                        error: format!(
                            "username:{} is not existed in database with sql:[{}]",
                            user_str, sql
                        ),
                    });
                }
                let is_rolesuper: String = rows.get(0).unwrap().get("rolsuper");
                if is_rolesuper.to_lowercase() == "true" {
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Err(e) => Err(Error::from(e)),
        }
    }
}
