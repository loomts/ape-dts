use strum::{Display, EnumString, IntoStaticStr};

#[derive(Clone, Display, EnumString, IntoStaticStr, Debug)]
pub enum DbType {
    #[strum(serialize = "mysql")]
    Mysql,
    #[strum(serialize = "pg")]
    Pg,
    #[strum(serialize = "kafka")]
    Kafka,
    #[strum(serialize = "open_faas")]
    OpenFaas,
    #[strum(serialize = "foxlake")]
    Foxlake,
    #[strum(serialize = "mongo")]
    Mongo,
}

#[derive(Display, EnumString, IntoStaticStr)]
pub enum ExtractType {
    #[strum(serialize = "snapshot")]
    Snapshot,
    #[strum(serialize = "cdc")]
    Cdc,
    #[strum(serialize = "check_log")]
    CheckLog,
    #[strum(serialize = "struct")]
    Struct,
    #[strum(serialize = "basic")]
    Basic,
}

#[derive(EnumString, IntoStaticStr)]
pub enum SinkType {
    #[strum(serialize = "write")]
    Write,
    #[strum(serialize = "check")]
    Check,
    #[strum(serialize = "struct")]
    Struct,
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

#[derive(EnumString, IntoStaticStr, Display)]
pub enum PipelineType {
    #[strum(serialize = "basic")]
    Basic,
    #[strum(serialize = "transaction")]
    Transaction,
}

pub enum RouteType {
    Db,
    Tb,
}

#[derive(EnumString, Clone, IntoStaticStr)]
pub enum ConflictPolicyEnum {
    #[strum(serialize = "ignore")]
    Ignore,
    #[strum(serialize = "interrupt")]
    Interrupt,
}
