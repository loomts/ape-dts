use strum::{EnumString, IntoStaticStr};

#[derive(EnumString, IntoStaticStr, PartialEq, Clone)]
pub enum LogType {
    #[strum(serialize = "miss")]
    Miss,
    #[strum(serialize = "diff")]
    Diff,
    #[strum(serialize = "unknown")]
    Unknown,
}
