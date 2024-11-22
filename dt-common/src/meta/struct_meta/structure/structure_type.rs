use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

#[derive(Debug, Clone, PartialEq, Display, EnumString, IntoStaticStr, Serialize, Deserialize)]
pub enum StructureType {
    #[strum(serialize = "database")]
    Database,
    #[strum(serialize = "table")]
    Table,
    #[strum(serialize = "constraint")]
    Constraint,
    #[strum(serialize = "sequence")]
    Sequence,
    #[strum(serialize = "comment")]
    Comment,
    #[strum(serialize = "index")]
    Index,
    #[strum(serialize = "unknown")]
    Unknown,
}
