use async_trait::async_trait;

use crate::meta::check_result::CheckResult;

#[async_trait]
pub trait Prechecker {
    async fn build_connection(&mut self) -> anyhow::Result<CheckResult>;

    async fn check_database_version(&mut self) -> anyhow::Result<CheckResult>;

    async fn check_permission(&mut self) -> anyhow::Result<CheckResult>;

    async fn check_cdc_supported(&mut self) -> anyhow::Result<CheckResult>;

    async fn check_struct_existed_or_not(&mut self) -> anyhow::Result<CheckResult>;

    async fn check_table_structs(&mut self) -> anyhow::Result<CheckResult>;
}
