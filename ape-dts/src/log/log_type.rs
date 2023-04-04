use strum::{AsStaticStr, EnumString};

#[derive(EnumString, AsStaticStr, PartialEq, Clone)]
pub enum LogType {
    #[strum(serialize = "miss")]
    Miss,
    #[strum(serialize = "diff")]
    Diff,
    #[strum(serialize = "unknown")]
    Unknown,
}
