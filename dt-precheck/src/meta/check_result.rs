use dt_common::meta::db_enums::DbType;

use crate::error::Error;

use super::check_item::CheckItem;

#[derive(Debug, Clone)]
pub struct CheckResult {
    pub check_type_name: String,
    pub check_desc: String,
    pub is_validate: bool,
    pub error_msg: String,
    pub advise_msg: String,
}

#[allow(dead_code)]
impl CheckResult {
    pub fn build(check_item: CheckItem) -> Self {
        return Self {
            check_type_name: check_item.to_string(),
            check_desc: String::from(""),
            is_validate: true,
            error_msg: String::from(""),
            advise_msg: String::from(""),
        };
    }

    pub fn build_with(
        check_type_name: String,
        check_desc: String,
        is_validate: bool,
        error_msg: String,
        advise_msg: String,
    ) -> Self {
        return Self {
            check_type_name,
            check_desc,
            is_validate,
            error_msg,
            advise_msg,
        };
    }

    pub fn build_with_err(
        check_item: CheckItem,
        is_source: bool,
        db_type_option: Option<DbType>,
        err_option: Option<Error>,
    ) -> Self {
        let check_desc;
        let mut advise_msg = String::new();
        let mut source_or_sink = String::from("source");
        if !is_source {
            source_or_sink = String::from("sink");
        }
        let db_type = db_type_option.unwrap();
        match check_item {
            CheckItem::CheckDatabaseConnection => {
                check_desc = format!("check if the {} database can be connected.", source_or_sink);
                advise_msg = format!("(1)check whether the account password is correct.(2)check if the network configuration is correct.");
            }
            CheckItem::CheckIfDatabaseSupportCdc => {
                check_desc = format!(
                    "check whether the {} database configuration supports cdc",
                    source_or_sink
                );
                match db_type {
                    DbType::Mysql => advise_msg = format!("(1)open 'log_bin' configuration. (2)set 'binlog_format' configuration to 'row'. (3)set 'binlog_row_image' configuration to 'full'."),
                    DbType::Pg => advise_msg = format!("(1)set 'wal_level' configuration to 'logical'. (2)make sure that the number of 'max_replication_slots' configured is sufficient. (3)make sure that the number of 'max_wal_senders' configured is sufficient."),
                    _ => {}
                }
            }
            CheckItem::CheckAccountPermission => {
                // Todo:
                check_desc = format!("check account permission");
                advise_msg = format!("advise account permission");
            }
            CheckItem::CheckIfStructExisted => {
                check_desc = format!(
                    "check whether the data structure of the {} database is existed",
                    source_or_sink
                );
                advise_msg = format!("manually created the missing struct.");
            }
            CheckItem::CheckIfTableStructSupported => {
                check_desc = format!(
                    "check whether the data structure of the {} database to be migrated supports",
                    source_or_sink
                );
                advise_msg = format!("no primary key tables and foreign key tables are currently not supported.these tables can be removed from the migration object.");
            }
            CheckItem::CheckDatabaseVersionSupported => {
                check_desc = format!("check if the {} database version supports.", source_or_sink);
                let mut advise_version = String::new();
                match db_type {
                    DbType::Mysql => advise_version = format!("currently supports version '8.*'."),
                    DbType::Pg => advise_version = format!("currently supports version '14.*'."),
                    _ => {}
                }
                advise_msg = format!("{} wait for the next release.", advise_version);
            }
        }
        match err_option {
            Some(err) => Self {
                check_type_name: check_item.to_string(),
                check_desc,
                is_validate: false,
                error_msg: err.to_string(),
                advise_msg,
            },
            None => Self {
                check_type_name: check_item.to_string(),
                check_desc,
                is_validate: true,
                error_msg: String::from(""),
                advise_msg: String::from(""),
            },
        }
    }

    pub fn log(&self) {
        println!("======================================");
        println!("[check_type_name]:{} \n[is_validate]:{} \n[check_desc]:{} \n[error_messaeg]:{} \n[advise_message]:{}\n", self.check_type_name, self.is_validate, self.check_desc, self.error_msg, self.advise_msg);
    }
}
