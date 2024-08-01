use std::{i64, path::PathBuf, str::FromStr};

use anyhow::Context;
use async_trait::async_trait;
use dt_common::{
    config::s3_config::S3Config,
    log_debug, log_info, log_warn,
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
const WAIT_FILE_SECS: u64 = 5;

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
        log_info!("start extracting from: {:?}", start_after);
        loop {
            let mut finished = false;
            let meta_files = self.list_meta_file(&start_after).await?;

            if !Self::check_continuity(&meta_files, &start_after) {
                log_warn!(
                    "meta files are not continuous, start_after: {:?}, meta_files: {}",
                    start_after,
                    meta_files.join(",")
                );
                TimeUtil::sleep_millis(WAIT_FILE_SECS * 1000).await;
                continue;
            }

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

            if finished {
                let dt_data = DtData::Foxlake {
                    file_meta: S3FileMeta {
                        push_epoch: i64::MAX,
                        ..Default::default()
                    },
                };
                self.base_extractor
                    .push_dt_data(dt_data, Position::None)
                    .await?;
                break;
            }

            if meta_files.is_empty() {
                TimeUtil::sleep_millis(WAIT_FILE_SECS * 1000).await;
            } else {
                start_after = meta_files.last().map(|s: &String| s.to_string());
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

    async fn list_meta_file(&self, start_after: &Option<String>) -> anyhow::Result<Vec<String>> {
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

    fn check_continuity(meta_files: &[String], start_after: &Option<String>) -> bool {
        let mut prev_meta_file = &String::new();
        let (mut prev_id, mut prev_sequence) = (0, 0);
        if let Some(v) = start_after {
            (prev_id, prev_sequence) = Self::parse_meta_file_name(v);
            prev_meta_file = v;
        }

        let mut continuous = true;
        for i in 0..meta_files.len() {
            let meta_file = &meta_files[i];
            // finished file
            if i == meta_files.len() - 1 && meta_file.ends_with(FINISHED) {
                continue;
            }

            let (id, sequence) = Self::parse_meta_file_name(meta_file);
            if id == 0 || id < prev_id {
                return false;
            }

            if id != prev_id {
                // This is the first file pushed by the same id, which means the pusher progress has restarted.
                // Abnormal exit of the previous pusher may lead to the discontinuity of the file sequence.
                // Ignore the discontinuity of previous files since they were pushed by previous pusher.
                if prev_id != 0 && sequence != 0 {
                    return false;
                }

                log_info!(
                    "found new id, previous meta file: {}, current meta file: {}",
                    prev_meta_file,
                    meta_file
                );

                prev_id = id;
                prev_sequence = sequence;
                prev_meta_file = meta_file;
                continuous = true;
                continue;
            }

            // discontinuity is caused by multiple threads pushing orc files in pusher progress.
            if sequence != prev_sequence + 1 {
                continuous = false;
                log_warn!(
                    "sequence discontinuity, previous meta file: {}, current meta file: {}",
                    prev_meta_file,
                    meta_file
                )
            }

            prev_sequence = sequence;
            prev_meta_file = meta_file;
        }

        continuous
    }

    fn parse_meta_file_name(meta_file: &str) -> (u64, u64) {
        if let Some(file_name) = PathBuf::from(meta_file).file_name() {
            if let Some(file_name_str) = file_name.to_str() {
                let parts: Vec<&str> = file_name_str.split('_').collect();
                if parts.len() >= 2 {
                    let id = parts[0].parse::<u64>().unwrap_or_default();
                    let sequence = parts[1].parse::<u64>().unwrap_or_default();
                    return (id, sequence);
                }
            }
        }
        (0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::FoxlakeS3Extractor;

    #[test]
    fn test_parse_meta_file_name() {
        let meta_file =
            "data/meta/1721796946_0000000012_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc";
        let (id, sequence) = FoxlakeS3Extractor::parse_meta_file_name(meta_file);
        assert_eq!(id, 1721796946);
        assert_eq!(sequence, 12);
    }

    #[test]
    fn test_check_continuity() {
        let transfer =
            |input: Vec<&str>| -> Vec<String> { input.iter().map(|s| s.to_string()).collect() };

        // start_after = None
        // case 1
        let meta_files = vec![
            "data/meta/1721796946_0000000000_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000001_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        assert_eq!(
            FoxlakeS3Extractor::check_continuity(&transfer(meta_files), &None),
            true
        );

        // case 2
        let meta_files = vec![
            "data/meta/1721796946_0000000002_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000003_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        assert_eq!(
            FoxlakeS3Extractor::check_continuity(&transfer(meta_files), &None),
            true
        );

        // case 3
        let meta_files = vec![
            "data/meta/1721796946_0000000002_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000004_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        assert_eq!(
            FoxlakeS3Extractor::check_continuity(&transfer(meta_files), &None),
            false
        );

        // start_after != None
        // case 1
        let start_after = Some(
            "data/meta/1721796946_0000000010_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc"
                .to_string(),
        );
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000012_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        assert_eq!(
            FoxlakeS3Extractor::check_continuity(&transfer(meta_files), &start_after),
            true
        );

        // case 2
        let meta_files = vec![
            "data/meta/1721796946_0000000012_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000013_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        assert_eq!(
            FoxlakeS3Extractor::check_continuity(&transfer(meta_files), &start_after),
            false
        );

        // case 3
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000013_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        assert_eq!(
            FoxlakeS3Extractor::check_continuity(&transfer(meta_files), &start_after),
            false
        );

        // case 4
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000012_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000014_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000015_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        assert_eq!(
            FoxlakeS3Extractor::check_continuity(&transfer(meta_files), &start_after),
            false
        );

        // case 5
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000013_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000000_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000001_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        assert_eq!(
            FoxlakeS3Extractor::check_continuity(&transfer(meta_files), &start_after),
            true
        );

        // case 6
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000013_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000001_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000002_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        assert_eq!(
            FoxlakeS3Extractor::check_continuity(&transfer(meta_files), &start_after),
            false
        );

        // case 7
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000012_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000001_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000002_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        assert_eq!(
            FoxlakeS3Extractor::check_continuity(&transfer(meta_files), &start_after),
            false
        );

        // case 8
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000012_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000000_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000001_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000004_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        assert_eq!(
            FoxlakeS3Extractor::check_continuity(&transfer(meta_files), &start_after),
            false
        );

        // case 9
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000012_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000000_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000001_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000004_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800555_0000000000_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        assert_eq!(
            FoxlakeS3Extractor::check_continuity(&transfer(meta_files), &start_after),
            true
        );

        // case 10
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000012_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000000_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000001_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800418_0000000004_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800555_0000000000_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721800555_0000000002_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
        ];
        assert_eq!(
            FoxlakeS3Extractor::check_continuity(&transfer(meta_files), &start_after),
            false
        );

        // finished
        let meta_files = vec![
            "data/meta/1721796946_0000000011_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/1721796946_0000000012_log_dml_0_0_d6b38c60-c395-4a00-88ae-80025f81d52f.orc",
            "data/meta/finished",
        ];
        assert_eq!(
            FoxlakeS3Extractor::check_continuity(&transfer(meta_files), &start_after),
            true
        );

        // empty
        assert_eq!(
            FoxlakeS3Extractor::check_continuity(&Vec::new(), &start_after),
            true
        );
    }
}
