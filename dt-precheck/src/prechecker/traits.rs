use async_trait::async_trait;

use crate::{error::Error, meta::check_result::CheckResult};

#[async_trait]
pub trait Prechecker {
    async fn build_connection(&mut self) -> Result<CheckResult, Error>;

    async fn check_database_version(&mut self) -> Result<CheckResult, Error>;

    async fn check_permission(&mut self) -> Result<CheckResult, Error>;

    async fn check_cdc_supported(&mut self) -> Result<CheckResult, Error>;

    async fn check_struct_existed_or_not(&mut self) -> Result<CheckResult, Error>;

    async fn check_table_structs(&mut self) -> Result<CheckResult, Error>;
}
