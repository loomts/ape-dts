use std::collections::HashMap;

use async_trait::async_trait;
use dt_common::{error::Error, utils::rdb_filter::RdbFilter};
use dt_task::task_util::TaskUtil;
use futures::{Stream, TryStreamExt};
use sqlx::{postgres::PgRow, query, Pool, Postgres, Row};

use crate::{
    fetcher::traits::Fetcher,
    meta::database_mode::{Constraint, Database, Schema, Table},
};

pub struct PgFetcher {
    pub pool: Option<Pool<Postgres>>,
    pub url: String,
    pub is_source: bool,
    pub filter: RdbFilter,
}

#[async_trait]
impl Fetcher for PgFetcher {
    async fn build_connection(&mut self) -> Result<(), Error> {
        self.pool = Some(TaskUtil::create_pg_conn_pool(&self.url, 1, true).await?);
        Ok(())
    }

    async fn fetch_version(&mut self) -> Result<String, Error> {
        let sql = String::from("SELECT current_setting('server_version_num')::varchar");
        let mut version = String::from("");

        let results = self.fetch_all(sql, "pg query database version").await;
        match results {
            Ok(rows) => {
                if !rows.is_empty() {
                    version = rows.get(0).unwrap().get("current_setting");
                }
            }
            Err(e) => return Err(e),
        }
        Ok(version)
    }

    async fn fetch_configuration(
        &mut self,
        config_keys: Vec<String>,
    ) -> Result<HashMap<String, String>, Error> {
        if config_keys.is_empty() {
            return Ok(HashMap::new());
        }

        let sql = format!(
            "SELECT name,setting::varchar FROM pg_settings WHERE name in ({})",
            config_keys
                .iter()
                .map(|c| format!("'{}'", c))
                .collect::<Vec<_>>()
                .join(",")
        );

        let mut result_map: HashMap<String, String> = config_keys
            .iter()
            .map(|c| (c.clone(), String::from("")))
            .collect();
        let result = self.fetch_all(sql, "pg query config settings").await;
        match result {
            Ok(rows) => {
                for row in rows {
                    let (name, setting): (String, String) = (row.get("name"), row.get("setting"));
                    if result_map.contains_key(name.as_str()) {
                        result_map.insert(name, setting);
                    }
                }
            }
            Err(e) => return Err(e),
        }

        Ok(result_map)
    }

    async fn fetch_databases(&mut self) -> Result<Vec<Database>, Error> {
        Ok(vec![])
    }

    async fn fetch_schemas(&mut self) -> Result<Vec<Schema>, Error> {
        let mut schemas: Vec<Schema> = vec![];
        let sql = "select catalog_name,schema_name from information_schema.schemata";

        let rows_result = self.fetch_row(sql, "pg query schema sql");
        match rows_result {
            Ok(mut rows) => {
                while let Some(row) = rows.try_next().await.unwrap() {
                    let (database_name, schema_name): (String, String) =
                        (row.get("catalog_name"), row.get("schema_name"));
                    if !self.filter.filter_db(&schema_name) {
                        schemas.push(Schema {
                            database_name,
                            schema_name,
                        })
                    }
                }
            }
            Err(e) => return Err(e),
        }

        Ok(schemas)
    }

    async fn fetch_tables(&mut self) -> Result<Vec<Table>, Error> {
        let mut tables: Vec<Table> = vec![];
        let table_sql = "select distinct table_catalog, table_schema, table_name from information_schema.columns";

        let rows_result = self.fetch_row(table_sql, "pg query table sql");
        match rows_result {
            Ok(mut rows) => {
                while let Some(row) = rows.try_next().await.unwrap() {
                    let (database_name, schema_name, table_name): (String, String, String) = (
                        row.get("table_catalog"),
                        row.get("table_schema"),
                        row.get("table_name"),
                    );
                    if !self.filter.filter_tb(&schema_name, &table_name) {
                        tables.push(Table {
                            database_name,
                            schema_name,
                            table_name,
                        })
                    }
                }
            }
            Err(e) => return Err(e),
        }

        Ok(tables)
    }

    async fn fetch_constraints(&mut self) -> Result<Vec<Constraint>, Error> {
        let mut constraints: Vec<Constraint> = vec![];
        let sql = "SELECT nsp.nspname, rel.relname, con.conname as constraint_name, con.contype::varchar as constraint_type
        FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace";

        let rows_result = self.fetch_row(sql, "pg query constraint sql");
        match rows_result {
            Ok(mut rows) => {
                while let Some(row) = rows.try_next().await.unwrap() {
                    let (schema_name, table_name, constraint_name, constraint_type): (
                        String,
                        String,
                        String,
                        String,
                    ) = (
                        row.get("nspname"),
                        row.get("relname"),
                        row.get("constraint_name"),
                        row.get("constraint_type"),
                    );
                    if !self.filter.filter_tb(&schema_name, &table_name) {
                        constraints.push(Constraint {
                            database_name: String::from(""),
                            schema_name,
                            table_name,
                            column_name: String::from(""),
                            constraint_name,
                            constraint_type,
                        })
                    }
                }
            }
            Err(e) => return Err(e),
        }

        Ok(constraints)
    }
}

impl PgFetcher {
    async fn fetch_all(&self, sql: String, mut sql_msg: &str) -> Result<Vec<PgRow>, Error> {
        let pg_pool = match &self.pool {
            Some(pool) => pool,
            None => return Err(Error::from(sqlx::Error::PoolClosed)),
        };

        sql_msg = if sql_msg.is_empty() { "sql" } else { sql_msg };
        println!("{}: {}", sql_msg, sql);

        let rows_result = query(&sql).fetch_all(pg_pool).await;
        match rows_result {
            Ok(rows) => Ok(rows),
            Err(e) => Err(Error::from(e)),
        }
    }

    fn fetch_row<'a>(
        &self,
        sql: &'a str,
        mut sql_msg: &str,
    ) -> Result<impl Stream<Item = Result<PgRow, sqlx::Error>> + 'a, Error> {
        match &self.pool {
            Some(pool) => {
                sql_msg = if sql_msg.is_empty() { "sql" } else { sql_msg };
                println!("{}: {}", sql_msg, sql);
                Ok(query(sql).fetch(pool))
            }
            None => Err(Error::from(sqlx::Error::PoolClosed)),
        }
    }

    pub async fn fetch_slot_names(&self) -> Result<Vec<String>, Error> {
        let mut slots: Vec<String> = vec![];
        let slot_query = "select slot_name from pg_catalog.pg_replication_slots".to_string();

        let result = self.fetch_all(slot_query, "pg query slots").await;
        match result {
            Ok(rows) => {
                for row in rows {
                    let slot_name = row.get("slot_name");
                    slots.push(slot_name);
                }
            }
            Err(e) => return Err(e),
        }
        Ok(slots)
    }
}
