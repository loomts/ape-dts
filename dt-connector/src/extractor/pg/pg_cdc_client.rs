use dt_common::error::Error;
use dt_common::{log_info, log_warn};
use postgres_types::PgLsn;
use tokio_postgres::NoTls;
use tokio_postgres::SimpleQueryMessage::Row;
use tokio_postgres::{replication::LogicalReplicationStream, Client};
use url::Url;

pub struct PgCdcClient {
    pub url: String,
    pub slot_name: String,
    pub pub_name: String,
    pub start_lsn: String,
}

impl PgCdcClient {
    pub async fn connect(&mut self) -> Result<(LogicalReplicationStream, String), Error> {
        let url_info = Url::parse(&self.url).unwrap();
        let host = url_info.host_str().unwrap().to_string();
        let port = format!("{}", url_info.port().unwrap());
        let dbname = url_info.path().trim_start_matches('/');
        let username = url_info.username().to_string();
        let password = url_info.password().unwrap().to_string();
        let conn_info = format!(
            "host={} port={} dbname={} user={} password={} replication=database",
            host, port, dbname, username, password
        );

        let (client, connection) = tokio_postgres::connect(&conn_info, NoTls).await.unwrap();
        tokio::spawn(async move {
            log_info!("postgres replication connection starts",);
            if let Err(e) = connection.await {
                log_info!("postgres replication connection drops, error: {}", e);
            }
        });
        self.start_replication(&client).await
    }

    async fn start_replication(
        &mut self,
        client: &Client,
    ) -> Result<(LogicalReplicationStream, String), Error> {
        let mut start_lsn = self.start_lsn.clone();

        // create publication for all tables if not exists
        let pub_name = if self.pub_name.is_empty() {
            format!("{}_publication_for_all_tables", self.slot_name)
        } else {
            self.pub_name.clone()
        };
        let query = format!(
            "SELECT * FROM {} WHERE pubname = '{}'",
            "pg_catalog.pg_publication", pub_name
        );
        let res = client.simple_query(&query).await.unwrap();
        let pub_exists = res.len() > 1;
        log_info!("publication: {} exists: {}", pub_name, pub_exists);

        if !pub_exists {
            let query = format!("CREATE PUBLICATION {} FOR ALL TABLES", pub_name);
            log_info!("execute: {}", query);
            client.simple_query(&query).await.unwrap();
        }

        // check slot exists
        let query = format!(
            "SELECT * FROM {} WHERE slot_name = '{}'",
            "pg_catalog.pg_replication_slots", self.slot_name
        );
        let res = client.simple_query(&query).await.unwrap();
        let slot_exists = res.len() > 1;
        let mut create_slot = !slot_exists;
        log_info!("slot: {} exists: {}", self.slot_name, slot_exists);

        if slot_exists {
            let confirmed_flush_lsn = if let Row(row) = &res[0] {
                row.get("confirmed_flush_lsn").unwrap().to_string()
            } else {
                String::new()
            };
            log_info!("slot confirmed_flush_lsn: {}", confirmed_flush_lsn);

            if confirmed_flush_lsn.is_empty() {
                // should never happen
                create_slot = true;
                log_warn!("slot exists but confirmed_flush_lsn is empty, will recreate slot");
            } else {
                if start_lsn.is_empty() {
                    log_warn!("start_lsn is empty, will use confirmed_flush_lsn");
                    start_lsn = confirmed_flush_lsn;
                } else {
                    let actual_lsn: PgLsn = confirmed_flush_lsn.parse().unwrap();
                    let input_lsn: PgLsn = start_lsn.parse().unwrap();
                    if input_lsn < actual_lsn {
                        log_warn!("start_lsn: {} is order than confirmed_flush_lsn: {}, will use confirmed_flush_lsn", 
                        start_lsn, confirmed_flush_lsn);
                        start_lsn = confirmed_flush_lsn;
                    }
                }
            }
        }

        // create replication slot
        if create_slot {
            // should never happen
            if slot_exists {
                let query = format!(
                    "SELECT {} ('{}')",
                    "pg_drop_replication_slot", self.slot_name
                );
                log_info!("execute: {}", query);
                client.simple_query(&query).await.unwrap();
            }

            let query = format!(
                r#"CREATE_REPLICATION_SLOT {} LOGICAL "{}""#,
                self.slot_name, "pgoutput"
            );
            log_info!("execute: {}", query);

            let res = client.simple_query(&query).await.unwrap();
            // get the lsn for the newly created slot
            start_lsn = if let Row(row) = &res[0] {
                row.get("consistent_point").unwrap().to_string()
            } else {
                return Err(Error::ExtractorError(format!(
                    "failed to create replication slot by query: {}",
                    query
                )));
            };

            log_info!(
                "slot created, returned start_sln: {}",
                start_lsn.to_string()
            );
        }

        // start replication slot
        let options = format!(
            r#"("proto_version" '{}', "publication_names" '{}')"#,
            "1", pub_name
        );
        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} {}",
            self.slot_name, start_lsn, options
        );
        log_info!("execute: {}", query);

        let copy_stream = client
            .copy_both_simple::<bytes::Bytes>(&query)
            .await
            .unwrap();
        let stream = LogicalReplicationStream::new(copy_stream);
        Ok((stream, start_lsn))
    }
}
