use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

#[derive(Debug, Clone, PartialEq, Display, EnumString, IntoStaticStr, Serialize, Deserialize)]
pub enum DdlType {
    #[strum(serialize = "create_database")]
    CreateDatabase,
    #[strum(serialize = "drop_database")]
    DropDatabase,
    #[strum(serialize = "create_schema")]
    CreateSchema,
    #[strum(serialize = "drop_schema")]
    DropSchema,
    #[strum(serialize = "create_table")]
    CreateTable,
    #[strum(serialize = "drop_table")]
    DropTable,
    #[strum(serialize = "truncate_table")]
    TruncateTable,
    #[strum(serialize = "rename_table")]
    RenameTable,
    #[strum(serialize = "alter_database")]
    AlterDatabase,
    #[strum(serialize = "alter_schema")]
    AlterSchema,
    #[strum(serialize = "alter_table")]
    AlterTable,
    #[strum(serialize = "create_index")]
    CreateIndex,
    #[strum(serialize = "unknown")]
    Unknown,
}

impl Default for DdlType {
    fn default() -> Self {
        Self::Unknown
    }
}
