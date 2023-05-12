use std::str::FromStr;

use strum::{Display, EnumString, IntoStaticStr};

use crate::error::Error;

#[derive(Display, EnumString, IntoStaticStr)]
pub enum ExtractType {
    #[strum(serialize = "snapshot")]
    Snapshot,
    #[strum(serialize = "cdc")]
    Cdc,
    #[strum(serialize = "check_log")]
    CheckLog,
    #[strum(serialize = "basic")]
    Basic,
}

#[derive(EnumString, IntoStaticStr)]
pub enum SinkType {
    #[strum(serialize = "write")]
    Write,
    #[strum(serialize = "check")]
    Check,
    #[strum(serialize = "basic")]
    Basic,
}

#[derive(EnumString, IntoStaticStr, Clone, Display)]
pub enum ParallelType {
    #[strum(serialize = "serial")]
    Serial,
    #[strum(serialize = "snapshot")]
    Snapshot,
    #[strum(serialize = "rdb_partition")]
    RdbPartition,
    #[strum(serialize = "rdb_merge")]
    RdbMerge,
    #[strum(serialize = "rdb_check")]
    RdbCheck,
    #[strum(serialize = "table")]
    Table,
    #[strum(serialize = "mongo")]
    Mongo,
}

pub enum RouteType {
    Db,
    Tb,
}

#[derive(Clone, IntoStaticStr)]
pub enum ConflictPolicyEnum {
    #[strum(serialize = "ignore")]
    Ignore,
    #[strum(serialize = "interrupt")]
    Interrupt,
}

impl FromStr for ConflictPolicyEnum {
    type Err = Error;
    fn from_str(str: &str) -> Result<Self, Self::Err> {
        match str {
            "ignore" => Ok(Self::Ignore),
            _ => Ok(Self::Interrupt),
        }
    }
}
