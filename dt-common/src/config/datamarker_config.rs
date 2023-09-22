use strum::{Display, EnumString, IntoStaticStr};

#[derive(Clone)]
pub struct DataMarkerConfig {
    pub setting: Option<DataMarkerSettingEnum>,
}

#[derive(Clone)]
pub enum DataMarkerSettingEnum {
    Transaction {
        transaction_db: String,
        transaction_table: String,
        transaction_express: String,
        transaction_command: String,
        white_nodes: String,
        black_nodes: String,
    },
}

#[derive(Clone, Display, EnumString, IntoStaticStr)]
pub enum DataMarkerTypeEnum {
    #[strum(serialize = "transaction")]
    Transaction,
}
