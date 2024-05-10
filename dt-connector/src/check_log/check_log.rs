use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
};

use dt_common::error::Error;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::json;

use super::log_type::LogType;

#[derive(Serialize, Deserialize)]
pub struct CheckLog {
    pub log_type: LogType,
    pub schema: String,
    pub tb: String,
    #[serde(serialize_with = "ordered_map")]
    pub id_col_values: HashMap<String, Option<String>>,
    #[serde(serialize_with = "ordered_map")]
    pub diff_col_values: HashMap<String, DiffColValue>,
}

#[derive(Serialize, Deserialize)]
pub struct DiffColValue {
    pub src: Option<String>,
    pub dst: Option<String>,
}

impl std::fmt::Display for CheckLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

impl FromStr for CheckLog {
    type Err = Error;
    fn from_str(str: &str) -> Result<Self, Self::Err> {
        let me: Self = serde_json::from_str(str).unwrap();
        Ok(me)
    }
}

/// For use with serde's [serialize_with] attribute
pub fn ordered_map<S, K: Ord + Serialize, V: Serialize>(
    value: &HashMap<K, V>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let ordered: BTreeMap<_, _> = value.iter().collect();
    ordered.serialize(serializer)
}
