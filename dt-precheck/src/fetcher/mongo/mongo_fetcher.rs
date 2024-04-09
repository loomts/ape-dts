use std::collections::HashMap;

use async_trait::async_trait;
use dt_common::{error::Error, rdb_filter::RdbFilter};
use dt_task::task_util::TaskUtil;
use mongodb::{
    bson::{doc, Bson, Document},
    Client,
};

use crate::{
    fetcher::traits::Fetcher,
    meta::database_mode::{Constraint, Database, Schema, Table},
};

pub struct MongoFetcher {
    pub pool: Option<Client>,
    pub url: String,
    pub is_source: bool,
    pub filter: RdbFilter,
}

#[async_trait]
impl Fetcher for MongoFetcher {
    async fn build_connection(&mut self) -> Result<(), Error> {
        self.pool = Some(TaskUtil::create_mongo_client(&self.url, "").await?);
        Ok(())
    }

    async fn fetch_version(&mut self) -> Result<String, Error> {
        let document = self.execute_for_db("buildInfo").await?;
        Ok(String::from(
            document
                .get("version")
                .and_then(Bson::as_str)
                .unwrap_or("unknown"),
        ))
    }

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

impl MongoFetcher {
    pub async fn execute_for_db(&self, command: &str) -> Result<Document, Error> {
        let client = match &self.pool {
            Some(pool) => pool,
            None => return Err(Error::PreCheckError("client is closed.".into())),
        };

        let dbs = client.list_databases(None, None).await?;
        if dbs.is_empty() {
            return Err(Error::PreCheckError("no db exists in mongo.".into()));
        }

        let doc_command = doc! {command: 1};
        let doc = client
            .database(&dbs[0].name)
            .run_command(doc_command, None)
            .await?;
        Ok(doc)
    }
}
