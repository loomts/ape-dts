use async_trait::async_trait;

use crate::{error::Error, meta::check_result::CheckResult};

#[async_trait]
// Todo: make a trait to describe the database method, such as: get_table, get_schema, get_index and so-on, use this method to build the checker
pub trait Checker {
    async fn build_connection(&mut self) -> Result<CheckResult, Error>;

    async fn check_database_version(&self) -> Result<CheckResult, Error>;

    async fn check_permission(&self) -> Result<CheckResult, Error>;

    async fn check_cdc_supported(&self) -> Result<CheckResult, Error>;

    async fn check_struct_existed_or_not(&self) -> Result<CheckResult, Error>;

    async fn check_table_structs(&self) -> Result<CheckResult, Error>;
}
