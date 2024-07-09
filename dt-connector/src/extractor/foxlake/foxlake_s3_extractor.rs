use std::str::FromStr;

use anyhow::Context;
use async_trait::async_trait;
use dt_common::{
    config::s3_config::S3Config,
    log_debug, log_info,
    meta::{dt_data::DtData, foxlake::s3_file_meta::S3FileMeta, position::Position},
    utils::time_util::TimeUtil,
};
use rusoto_s3::{GetObjectRequest, ListObjectsV2Request, S3Client, S3};
use tokio::io::AsyncReadExt;

use crate::{
    extractor::{base_extractor::BaseExtractor, resumer::snapshot_resumer::SnapshotResumer},
    Extractor,
};

pub struct FoxlakeS3Extractor {
    pub s3_client: S3Client,
    pub s3_config: S3Config,
    pub base_extractor: BaseExtractor,
    pub resumer: SnapshotResumer,
    pub slice_size: usize,
    pub schema: String,
    pub tb: String,
}

const FINISHED: &str = "finished";

#[async_trait]
impl Extractor for FoxlakeS3Extractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!(
            "FoxlakeS3Extractor starts, schema: `{}`, tb: `{}`, slice_size: {}",
            self.schema,
            self.tb,
            self.slice_size
        );
        self.extract_internal().await?;
        self.base_extractor.wait_task_finish().await
    }
}

impl FoxlakeS3Extractor {
    async fn extract_internal(&mut self) -> anyhow::Result<()> {
        let mut start_after = self.resumer.get_resume_value(&self.schema, &self.tb, "");
        loop {
            let mut finished = false;
            let meta_files = self.list_meta_file(start_after).await?;
            for file in meta_files.iter() {
                if file.ends_with(FINISHED) {
                    finished = true;
                    break;
                }
                let file_meta = self.get_meta(file).await?;
                let dt_data = DtData::Foxlake { file_meta };
                let position = Position::FoxlakeS3 {
                    schema: self.schema.clone(),
                    tb: self.tb.clone(),
                    s3_meta_file: file.to_owned(),
                };
                self.base_extractor.push_dt_data(dt_data, position).await?;
            }

            start_after = meta_files.last().map(|s: &String| s.to_string());
            if finished {
                break;
            }

            if meta_files.is_empty() {
                TimeUtil::sleep_millis(5000).await;
            }
        }
        Ok(())
    }

    async fn get_meta(&self, key: &str) -> anyhow::Result<S3FileMeta> {
        let request = GetObjectRequest {
            bucket: self.s3_config.bucket.clone(),
            key: key.to_string(),
            ..Default::default()
        };
        let output = self
            .s3_client
            .get_object(request)
            .await
            .with_context(|| format!("failed to get s3 object, key: {}", &key))?;

        let mut content = String::new();
        if let Some(body) = output.body {
            body.into_async_read().read_to_string(&mut content).await?;
        }

        S3FileMeta::from_str(&content).with_context(|| format!("invalid s3 file meta: [{}]", key))
    }

    async fn list_meta_file(&self, start_after: Option<String>) -> anyhow::Result<Vec<String>> {
        let mut prefix = format!("{}/{}/meta/", self.schema, self.tb);
        if !self.s3_config.root_dir.is_empty() {
            prefix = format!("{}/{}", self.s3_config.root_dir, prefix);
        }

        log_debug!(
            "list meta files, prefix: {}, start_after: {:?}",
            &prefix,
            start_after
        );
        let request = ListObjectsV2Request {
            bucket: self.s3_config.bucket.clone(),
            max_keys: Some(self.slice_size as i64),
            prefix: Some(prefix.clone()),
            start_after: start_after.clone(),
            ..Default::default()
        };
        let output = self
            .s3_client
            .list_objects_v2(request)
            .await
            .with_context(|| {
                format!(
                    "failed to list s3 objects, prefix: {}, start_after: {:?}",
                    &prefix, start_after
                )
            })?;

        let mut file_names = Vec::new();
        if let Some(contents) = output.contents {
            for i in contents {
                if let Some(key) = i.key {
                    file_names.push(key);
                }
            }
        }

        log_debug!(
            "found meta files, count: {}, last file: {:?}",
            file_names.len(),
            file_names.last()
        );
        Ok(file_names)
    }
}
