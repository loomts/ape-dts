use async_trait::async_trait;
use log::info;
use rusoto_core::ByteStream;
use rusoto_s3::{GetObjectRequest, HeadObjectRequest, PutObjectRequest, S3Client, S3};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::{
    error::Error,
    meta::{ddl_data::DdlData, row_data::RowData},
    traits::Sinker,
};

pub struct FoxlakeSinker {
    pub batch_size: usize,
    pub bucket: String,
    pub root_dir: String,
    pub s3_client: S3Client,
}

#[async_trait]
impl Sinker for FoxlakeSinker {
    async fn sink_dml(&mut self, data: Vec<RowData>, _batch: bool) -> Result<(), Error> {
        if data.len() == 0 {
            return Ok(());
        }

        let db = &data[0].schema;
        let tb = &data[0].tb;
        let sequence = self.get_sequence(db).await.unwrap();
        let key = self.generate_dml_key(db, tb, sequence);

        let mut content = String::new();
        for i in data.iter() {
            content.push_str(&i.to_string());
            content.push_str("\n");
        }
        self.put_to_file(&key, &content).await.unwrap();
        Ok(())
    }

    async fn sink_ddl(&mut self, data: Vec<DdlData>, _batch: bool) -> Result<(), Error> {
        if data.len() == 0 {
            return Ok(());
        }

        let db = &data[0].schema;

        // update sequence
        let next_sequence = self.get_sequence(db).await.unwrap() + 1;
        self.update_sequence(db, next_sequence).await.unwrap();

        // push ddl file to new sequence folder
        let key = self.generate_ddl_key(db, next_sequence);
        let mut content = String::new();
        for i in data.iter() {
            content.push_str(&i.to_string());
            content.push_str("\n");
        }
        self.put_to_file(&key, &content).await.unwrap();
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl FoxlakeSinker {
    fn generate_ddl_key(&self, db: &str, sequence: u32) -> String {
        let file_name = format!("binlog-ddl-{}", Uuid::new_v4());
        format!("{}/{}/{}/{}", self.root_dir, db, sequence, file_name)
    }

    fn generate_dml_key(&self, db: &str, tb: &str, sequence: u32) -> String {
        let file_name = format!("binlog-{}", Uuid::new_v4());
        format!("{}/{}/{}/{}/{}", self.root_dir, db, sequence, tb, file_name)
    }

    async fn put_to_file(&self, key: &str, content: &str) -> Result<(), Error> {
        let byte_stream = ByteStream::from(content.as_bytes().to_vec());
        let request = PutObjectRequest {
            bucket: self.bucket.clone(),
            key: key.to_string(),
            body: Some(byte_stream),
            ..Default::default()
        };
        self.s3_client.put_object(request).await.unwrap();
        Ok(())
    }

    async fn check_file_exists(&self, key: &str) -> Result<bool, Error> {
        let request = HeadObjectRequest {
            bucket: self.bucket.clone(),
            key: key.to_string(),
            ..Default::default()
        };

        match self.s3_client.head_object(request).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    fn get_sequence_key(&self, db: &str) -> String {
        format!("{}/{}/sequence", self.root_dir, db)
    }

    async fn get_sequence(&self, db: &str) -> Result<u32, Error> {
        let key = self.get_sequence_key(db);
        if self.check_file_exists(&key).await.unwrap() {
            let request = GetObjectRequest {
                bucket: self.bucket.clone(),
                key,
                ..Default::default()
            };

            let result = self.s3_client.get_object(request).await.unwrap();
            let mut content = String::new();
            let body = result.body.unwrap();
            let mut async_read = body.into_async_read();
            async_read.read_to_string(&mut content).await.unwrap();
            return Ok(content.parse::<u32>().unwrap());
        }
        Ok(0)
    }

    async fn update_sequence(&self, db: &str, sequence: u32) -> Result<(), Error> {
        let key = format!("{}/{}/sequence", self.root_dir, db);
        info!("update sequece, db:{}, sequence: {}", db, sequence);
        self.put_to_file(&key, &sequence.to_string()).await
    }
}
