use std::collections::HashSet;

use async_trait::async_trait;
use dt_common::{
    config::{config_enums::DbType, filter_config::FilterConfig},
    error::Error,
};
use dt_meta::struct_meta::{db_table_model::DbTable, pg_enums::ConstraintTypeEnum};

use crate::{
    config::precheck_config::PrecheckConfig,
    fetcher::{postgresql::pg_fetcher::PgFetcher, traits::Fetcher},
    meta::check_item::CheckItem,
    meta::check_result::CheckResult,
};

use super::traits::Prechecker;

const PG_SUPPORT_DB_VERSION_NUM_MIN: i32 = 120000;
const PG_SUPPORT_DB_VERSION_NUM_MAX: i32 = 149999;

pub struct PostgresqlPrechecker {
    pub fetcher: PgFetcher,
    pub filter_config: FilterConfig,
    pub precheck_config: PrecheckConfig,
    pub is_source: bool,
}

#[async_trait]
impl Prechecker for PostgresqlPrechecker {
    async fn build_connection(&mut self) -> Result<CheckResult, Error> {
        let mut check_error = None;
        let result = self.fetcher.build_connection().await;
        match result {
            Ok(_) => {}
            Err(e) => check_error = Some(e),
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckDatabaseConnection,
            self.is_source,
            DbType::Pg,
            check_error,
        ))
    }

    // Supported PostgreSQL 14.*
    async fn check_database_version(&mut self) -> Result<CheckResult, Error> {
        let mut check_error: Option<Error> = None;

        let result = self.fetcher.fetch_version().await;
        match result {
            Ok(version) => {
                if version.is_empty() {
                    check_error = Some(Error::PreCheckError("found no version info".into()));
                } else {
                    let version_i32: i32 = version.parse().unwrap();
                    if !(PG_SUPPORT_DB_VERSION_NUM_MIN..=PG_SUPPORT_DB_VERSION_NUM_MAX)
                        .contains(&version_i32)
                    {
                        check_error = Some(Error::PreCheckError(format!(
                            "version:{} is not supported yet",
                            version_i32
                        )));
                    }
                }
            }
            Err(e) => check_error = Some(e),
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckDatabaseVersionSupported,
            self.is_source,
            DbType::Pg,
            check_error,
        ))
    }

    async fn check_permission(&mut self) -> Result<CheckResult, Error> {
        Ok(CheckResult::build(
            CheckItem::CheckAccountPermission,
            self.is_source,
        ))
    }

    async fn check_cdc_supported(&mut self) -> Result<CheckResult, Error> {
        let mut check_error: Option<Error> = None;

        if !self.is_source {
            // do nothing when the database is target
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckIfDatabaseSupportCdc,
                self.is_source,
                DbType::Pg,
                check_error,
            ));
        }

        // check the cdc settings
        let configs: Vec<String> = vec!["wal_level", "max_wal_senders", "max_replication_slots"]
            .iter()
            .map(|c| c.to_string())
            .collect();
        let (mut max_replication_slots_i32, mut err_msgs): (i32, Vec<String>) = (0, vec![]);
        let result = self.fetcher.fetch_configuration(configs).await;
        match result {
            Ok(rows) => {
                for (k, v) in rows {
                    match k.as_str() {
                        "wal_level" => {
                            if v.to_lowercase() != "logical" {
                                err_msgs.push(format!(
                                    "wal_level should not be '{}', need to be 'logical'.",
                                    v
                                ))
                            }
                        }
                        "max_replication_slots" => {
                            max_replication_slots_i32 = v.parse().unwrap();
                            if max_replication_slots_i32 < 1 {
                                err_msgs.push(format!(
                                    "max_replication_slots needs to be greater than 0. current is '{}'",
                                    max_replication_slots_i32
                                ))
                            }
                        }
                        "max_wal_senders" => {
                            let sender_i32: i32 = v.parse().unwrap();
                            if sender_i32 < 1 {
                                err_msgs.push(format!(
                                    "max_wel_senders needs to be greater than 0, current is '{}'",
                                    sender_i32
                                ))
                            }
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => return Err(e),
        }
        if !err_msgs.is_empty() {
            check_error = Some(Error::PreCheckError(err_msgs.join(";").into()));
        }

        if check_error.is_none() {
            // check the slot count is less than max_replication_slots or not
            let slot_result = self.fetcher.fetch_slot_names().await;
            match slot_result {
                Ok(slots) => {
                    if max_replication_slots_i32 == (slots.len() as i32) {
                        check_error = Some(Error::PreCheckError ( format!("the current number of slots:[{}] has reached max_replication_slots, and new slots cannot be created", max_replication_slots_i32) ));
                    }
                }
                Err(e) => check_error = Some(e),
            }
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfDatabaseSupportCdc,
            self.is_source,
            DbType::Pg,
            check_error,
        ))
    }

    async fn check_struct_existed_or_not(&mut self) -> Result<CheckResult, Error> {
        let mut check_error: Option<Error> = None;

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

        if self.is_source || !self.precheck_config.do_struct_init {
            // When a specific table to be migrated is specified and the following conditions are met, check the existence of the table
            // 1. this check is for the source database
            // 2. this check is for the sink database, and specified no structure initialization
            if !tbs.is_empty() {
                let mut not_existed_tbs: HashSet<String> = HashSet::new();

                let table_result = self.fetcher.fetch_tables().await;
                let current_tbs: HashSet<String> = match table_result {
                    Ok(tables) => tables
                        .iter()
                        .map(|t| format!("{}.{}", t.schema_name, t.table_name))
                        .collect(),
                    Err(e) => return Err(e),
                };
                for tb_key in tbs {
                    if !current_tbs.contains(&tb_key) {
                        not_existed_tbs.insert(tb_key);
                    }
                }
                if !not_existed_tbs.is_empty() {
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

            if !all_schemas.is_empty() {
                let mut not_existed_schema: HashSet<String> = HashSet::new();
                let schema_result = self.fetcher.fetch_schemas().await;
                let current_schemas: HashSet<String> = match schema_result {
                    Ok(schemas) => schemas.iter().map(|s| s.schema_name.clone()).collect(),
                    Err(e) => return Err(e),
                };

                for schema in all_schemas {
                    if !current_schemas.contains(schema) {
                        not_existed_schema.insert(schema.clone());
                    }
                }
                if !not_existed_schema.is_empty() {
                    err_msgs.push(format!(
                        "schemas not existed: [{}]",
                        not_existed_schema
                            .iter()
                            .map(|e| e.to_string())
                            .collect::<Vec<String>>()
                            .join(";")
                    ));
                }
            }
        }

        if !err_msgs.is_empty() {
            check_error = Some(Error::PreCheckError(err_msgs.join(".").into()))
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfStructExisted,
            self.is_source,
            DbType::Pg,
            check_error,
        ))
    }

    async fn check_table_structs(&mut self) -> Result<CheckResult, Error> {
        // all tables have a pk, and have no fk
        let mut check_error: Option<Error> = None;

        if !self.is_source {
            // do nothing when the database is a target
            return Ok(CheckResult::build_with_err(
                CheckItem::CheckIfTableStructSupported,
                self.is_source,
                DbType::Pg,
                check_error,
            ));
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
        let (schemas, tb_schemas, _) = DbTable::get_config_maps(&db_tables).unwrap();
        let mut all_schemas = Vec::new();
        all_schemas.extend(&schemas);
        all_schemas.extend(&tb_schemas);
        if all_schemas.is_empty() {
            println!("found no schema need to do migrate, very strange");
            return Err(Error::PreCheckError(
                "found no schema need to do migrate".into(),
            ));
        }

        let (mut has_pk_tables, mut has_fk_tables): (HashSet<String>, HashSet<String>) =
            (HashSet::new(), HashSet::new());

        let table_result = self.fetcher.fetch_tables().await;
        let current_tables: HashSet<String> = match table_result {
            Ok(tables) => tables
                .iter()
                .map(|t| format!("{}.{}", t.schema_name, t.table_name))
                .collect(),
            Err(e) => return Err(e),
        };

        let constraint_result = self.fetcher.fetch_constraints().await;
        match constraint_result {
            Ok(constraints) => {
                for c in constraints {
                    let schema_table_name = format!("{}.{}", c.schema_name, c.table_name);
                    if c.constraint_type == ConstraintTypeEnum::Primary.to_str().unwrap() {
                        has_pk_tables.insert(schema_table_name);
                    } else if c.constraint_type == ConstraintTypeEnum::Foregin.to_str().unwrap() {
                        has_fk_tables.insert(schema_table_name);
                    }
                }
            }
            Err(e) => return Err(e),
        }

        if !has_fk_tables.is_empty() {
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
        if !no_pk_tables.is_empty() {
            err_msgs.push(format!(
                "primary key are needed, but these tables don't have a primary key:[{}]",
                no_pk_tables
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<String>>()
                    .join(";")
            ));
        }
        if !err_msgs.is_empty() {
            check_error = Some(Error::PreCheckError(err_msgs.join(";").into()))
        }

        Ok(CheckResult::build_with_err(
            CheckItem::CheckIfTableStructSupported,
            self.is_source,
            DbType::Pg,
            check_error,
        ))
    }
}
