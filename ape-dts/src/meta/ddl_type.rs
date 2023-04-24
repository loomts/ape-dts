use serde::{Deserialize, Serialize};
use strum::{AsStaticStr, Display, EnumString};

#[derive(Debug, Clone, PartialEq, Display, EnumString, AsStaticStr, Serialize, Deserialize)]
pub enum DdlType {
    #[strum(serialize = "create")]
    Create,
    #[strum(serialize = "drop")]
    Drop,
    #[strum(serialize = "alter")]
    Alter,
    #[strum(serialize = "unknown")]
    Unknown,
}

impl Default for DdlType {
    fn default() -> Self {
        Self::Unknown
    }
}
