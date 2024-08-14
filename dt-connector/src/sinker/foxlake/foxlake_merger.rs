use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use anyhow::Context;
use async_trait::async_trait;
use dt_common::{
    config::{config_enums::ExtractType, s3_config::S3Config},
    log_debug, log_info,
    meta::dt_data::{DtData, DtItem},
    monitor::monitor::Monitor,
};
use rusoto_s3::S3Client;
use sqlx::{MySql, Pool};

use crate::{close_conn_pool, sinker::base_sinker::BaseSinker, Sinker};

pub struct FoxlakeMerger {
    pub batch_size: usize,
    pub monitor: Arc<Mutex<Monitor>>,
    pub s3_client: S3Client,
    pub s3_config: S3Config,
    pub conn_pool: Pool<MySql>,
    pub extract_type: ExtractType,
}

#[async_trait]
impl Sinker for FoxlakeMerger {
    async fn sink_raw(&mut self, data: Vec<DtItem>, _batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        self.batch_sink(data).await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        return close_conn_pool!(self);
    }
}

impl FoxlakeMerger {
    async fn batch_sink(&mut self, data: Vec<DtItem>) -> anyhow::Result<()> {
        let start_time = Instant::now();

        let (all_data_size, all_row_count) = self.batch_merge(data).await?;

        BaseSinker::update_batch_monitor(
            &mut self.monitor,
            all_row_count,
            all_data_size,
            start_time,
        )
        .await
    }

    pub async fn batch_merge(&mut self, data: Vec<DtItem>) -> anyhow::Result<(usize, usize)> {
        let mut all_row_count = 0;
        let mut all_data_size = 0;
        let mut schema = String::new();
        let mut tb = String::new();
        let mut s3_files = Vec::new();
        // let mut insert_only = true;

        for dt_item in data {
            if let DtData::Foxlake { file_meta } = dt_item.dt_data {
                all_row_count += file_meta.row_count;
                all_data_size += file_meta.data_size;
                schema = file_meta.schema;
                tb = file_meta.tb;
                s3_files.push(file_meta.data_file_name);
                // insert_only &= file_meta.insert_only;
            }
        }

        log_info!(
            "merging schema: {}, tb: {}, row_count: {}, data_size: {}, file_count: {}",
            schema,
            tb,
            all_row_count,
            all_data_size,
            s3_files.len()
        );

        let s3 = &self.s3_config;
        // minio: s3_endpoint=http://127.0.0.1:9000
        let endpoint = s3.endpoint.trim_start_matches("http://");

        let files: Vec<String> = s3_files.iter().map(|i| format!("'{}'", i)).collect();
        let addition = match self.extract_type {
            ExtractType::Snapshot | ExtractType::FoxlakeS3 => {
                "DEDUPLICATION = 'SOURCE' INSERT_ONLY = true".to_string()
            }
            _ => String::new(),
        };

        let sql = format!(
            r#"MERGE INTO TABLE `{}`.`{}` 
            USING URI = '{}/' 
            ENDPOINT = '{}' 
            CREDENTIALS = (ACCESS_KEY_ID='{}' SECRET_ACCESS_KEY='{}') 
            FILES=({}) FILE_FORMAT = (KIND='DML_CHANGE_LOG') {};"#,
            schema,
            tb,
            s3.root_url,
            endpoint,
            s3.access_key,
            s3.secret_key,
            files.join(","),
            addition
        );
        log_debug!("merge sql: {}", sql);

        let query = sqlx::query(&sql);
        query
            .execute(&self.conn_pool)
            .await
            .with_context(|| format!("merge to foxlake failed: {}", sql))?;

        log_info!("merge succeeded");
        Ok((all_data_size, all_row_count))
    }
}
