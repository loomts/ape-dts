use std::collections::HashMap;

use async_trait::async_trait;
use dt_common::error::Error;

use crate::meta::database_mode::{Constraint, Database, Schema, Table};

#[async_trait]
pub trait Fetcher {
    async fn build_connection(&mut self) -> Result<(), Error>;

    async fn fetch_version(&mut self) -> Result<String, Error>;

    async fn fetch_configuration(
        &mut self,
        _config_keys: Vec<String>,
    ) -> Result<HashMap<String, String>, Error> {
        Ok(HashMap::new())
    }

    async fn fetch_databases(&mut self) -> Result<Vec<Database>, Error> {
        Ok(vec![])
    }

    async fn fetch_schemas(&mut self) -> Result<Vec<Schema>, Error> {
        Ok(vec![])
    }

    async fn fetch_tables(&mut self) -> Result<Vec<Table>, Error> {
        Ok(vec![])
    }

    async fn fetch_constraints(&mut self) -> Result<Vec<Constraint>, Error> {
        Ok(vec![])
    }
}
