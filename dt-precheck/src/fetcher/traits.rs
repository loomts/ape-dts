use std::collections::HashMap;

use async_trait::async_trait;

use crate::meta::database_mode::{Constraint, Database, Schema, Table};

#[async_trait]
pub trait Fetcher {
    async fn build_connection(&mut self) -> anyhow::Result<()>;

    async fn fetch_version(&mut self) -> anyhow::Result<String>;

    async fn fetch_configuration(
        &mut self,
        _config_keys: Vec<String>,
    ) -> anyhow::Result<HashMap<String, String>> {
        Ok(HashMap::new())
    }

    async fn fetch_databases(&mut self) -> anyhow::Result<Vec<Database>> {
        Ok(vec![])
    }

    async fn fetch_schemas(&mut self) -> anyhow::Result<Vec<Schema>> {
        Ok(vec![])
    }

    async fn fetch_tables(&mut self) -> anyhow::Result<Vec<Table>> {
        Ok(vec![])
    }

    async fn fetch_constraints(&mut self) -> anyhow::Result<Vec<Constraint>> {
        Ok(vec![])
    }
}
