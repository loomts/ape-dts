use std::collections::HashMap;

use async_trait::async_trait;
use dt_common::{
    config::{
        config_enums::DbType, extractor_config::ExtractorConfig, filter_config::FilterConfig,
        router_config::RouterConfig, sinker_config::SinkerConfig,
    },
    constants::MongoConstants,
    utils::rdb_filter::RdbFilter,
};
use mongodb::{
    bson::{doc, Bson, Document},
    options::ClientOptions,
    Client,
};

use crate::{
    error::Error,
    fetcher::traits::Fetcher,
    meta::database_mode::{Constraint, Database, Schema, Table},
};

pub struct MongoFetcher {
    pub pool: Option<Client>,
    pub source_config: ExtractorConfig,
    pub filter_config: FilterConfig,
    pub sinker_config: SinkerConfig,
    pub router_config: RouterConfig,
    pub is_source: bool,
    pub db_type_option: Option<DbType>,
    pub filter: RdbFilter,
}

#[async_trait]
impl Fetcher for MongoFetcher {
    async fn build_connection(&mut self) -> Result<(), Error> {
        let mut connection_url = String::from("");

        if self.is_source {
            if let ExtractorConfig::Basic { url, db_type } = &self.source_config {
                connection_url = String::from(url);
                self.db_type_option = Some(db_type.to_owned());
            }
        } else if let SinkerConfig::Basic { url, db_type } = &self.sinker_config {
            connection_url = String::from(url);
            self.db_type_option = Some(db_type.to_owned());
        }

        let mut client_options = ClientOptions::parse_async(connection_url).await.unwrap();
        client_options.app_name = Some(MongoConstants::APP_NAME.to_string());
        self.pool = match mongodb::Client::with_options(client_options) {
            Ok(pool) => Some(pool),
            Err(e) => return Err(Error::from(e)),
        };

        Ok(())
    }

    async fn fetch_version(&mut self) -> Result<String, Error> {
        let db = match self.get_random_db().await {
            Ok(db_name) => db_name,
            Err(e) => return Err(e),
        };

        let document = self.execute_for_db(db, "buildInfo").await?;
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
    pub async fn execute_for_db(&self, db: String, command: &str) -> Result<Document, Error> {
        if db.is_empty() || command.is_empty() {
            return Ok(Document::default());
        }

        let client = match &self.pool {
            Some(pool) => pool,
            None => {
                return Err(Error::Unexpected {
                    error: String::from("client is closed."),
                })
            }
        };

        let doc_command = doc! {command: 1};
        let command_result = client
            .database(db.as_str())
            .run_command(doc_command, None)
            .await;

        match command_result {
            Ok(rs) => Ok(rs),
            Err(e) => Err(Error::from(e)),
        }
    }

    pub async fn get_random_db(&mut self) -> Result<String, Error> {
        let db = String::from("");

        let client = match &self.pool {
            Some(pool) => pool,
            None => {
                return Err(Error::Unexpected {
                    error: String::from("client is closed."),
                })
            }
        };
        let databases_result = client.list_database_names(None, None).await;
        match databases_result {
            Ok(databases) => {
                for db in databases {
                    if !&self.filter.filter_db(db.as_str()) {
                        return Ok(db);
                    }
                }
            }
            Err(e) => return Err(Error::from(e)),
        }
        Ok(db)
    }
}
