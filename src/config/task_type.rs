use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum TaskType {
    MysqlToMysqlSnapshot,
    MysqlToMysqlCdc,
}
