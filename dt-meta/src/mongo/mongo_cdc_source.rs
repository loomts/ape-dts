use std::str::FromStr;

use dt_common::error::Error;
use strum::IntoStaticStr;

#[derive(Clone, IntoStaticStr, Debug)]
pub enum MongoCdcSource {
    #[strum(serialize = "op_log")]
    OpLog,

    #[strum(serialize = "change_stream")]
    ChangeStream,
}

impl FromStr for MongoCdcSource {
    type Err = Error;
    fn from_str(str: &str) -> Result<Self, Self::Err> {
        match str {
            "op_log" => Ok(Self::OpLog),
            _ => Ok(Self::ChangeStream),
        }
    }
}
