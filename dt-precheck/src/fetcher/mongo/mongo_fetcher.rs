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
        let db = match self.get_random_db() {
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

    pub fn get_random_db(&mut self) -> Result<String, Error> {
        let mut db = String::from("");

        match &self.filter_config {
            FilterConfig::Rdb { do_dbs, do_tbs, .. } => {
                if !do_dbs.is_empty() {
                    for do_db in do_dbs.split(',') {
                        if !self.filter.filter_db(do_db) {
                            db = String::from(do_db);
                            break;
                        }
                    }
                }
                if db.is_empty() && !do_tbs.is_empty() {
                    for do_tb in do_tbs.split(',') {
                        let do_tb_string = do_tb.to_string();
                        let db_tb_vec_tmp: Vec<&str> = do_tb_string.split('.').collect();
                        if db_tb_vec_tmp.len() == 2
                            && !self.filter.filter_tb(db_tb_vec_tmp[0], db_tb_vec_tmp[1])
                        {
                            db = String::from(db_tb_vec_tmp[0]);
                            break;
                        }
                    }
                };
            }
        }
        if db == "*" {
            db = String::from("admin");
        }

        Ok(db)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_mongo_fetcher(
        do_dbs: &str,
        do_tbs: &str,
        ignore_dbs: &str,
        ignore_tbs: &str,
    ) -> MongoFetcher {
        let filter_config = FilterConfig::Rdb {
            do_dbs: String::from(do_dbs),
            ignore_dbs: String::from(ignore_dbs),
            do_tbs: String::from(do_tbs),
            ignore_tbs: String::from(ignore_tbs),
            do_events: String::from("insert,update,delete"),
        };

        MongoFetcher {
            pool: None,
            source_config: ExtractorConfig::Basic {
                url: String::from(""),
                db_type: DbType::Mongo,
            },
            filter_config: filter_config.clone(),
            sinker_config: SinkerConfig::Basic {
                url: String::from(""),
                db_type: DbType::Mongo,
            },
            router_config: RouterConfig::Rdb {
                db_map: String::from(""),
                tb_map: String::from(""),
                field_map: String::from(""),
            },
            is_source: true,
            db_type_option: Some(DbType::Mongo),
            filter: RdbFilter::from_config(&filter_config, DbType::Mongo).unwrap(),
        }
    }

    #[test]
    fn get_random_db_test() {
        let mut target_db: String;

        target_db = generate_mongo_fetcher("db1,db2,db3", "", "", "")
            .get_random_db()
            .unwrap();
        assert_eq!(target_db, "db1");

        target_db = generate_mongo_fetcher("db1,db2,db3", "", "db1,db2", "")
            .get_random_db()
            .unwrap();
        assert_eq!(target_db, "db3");

        target_db = generate_mongo_fetcher(
            "db1,db2",
            "db1.table1,db3.table1,db4.table2",
            "db1,db2",
            "db3.table1",
        )
        .get_random_db()
        .unwrap();
        assert_eq!(target_db, "db4");

        target_db = generate_mongo_fetcher("*", "", "db1,db2", "")
            .get_random_db()
            .unwrap();
        assert_eq!(target_db, "admin");

        target_db = generate_mongo_fetcher("", "db1.table1,*.*", "db1", "")
            .get_random_db()
            .unwrap();
        assert_eq!(target_db, "admin");
    }
}
