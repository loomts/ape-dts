use async_trait::async_trait;
use log::error;
use serde_json::json;

use crate::{
    error::Error,
    meta::{ddl_data::DdlData, row_data::RowData},
    traits::Sinker,
};

use reqwest::Client;

pub struct OpenFaasSinker {
    pub batch_size: usize,
    pub gateway_url: String,
    pub http_client: Client,
}

#[async_trait]
impl Sinker for OpenFaasSinker {
    async fn sink_dml(&mut self, data: Vec<RowData>, _batch: bool) -> Result<(), Error> {
        let all_count = data.len();
        let mut sinked_count = 0;

        loop {
            let mut batch_size = self.batch_size;
            if all_count - sinked_count < batch_size {
                batch_size = all_count - sinked_count;
            }

            let mut messages = Vec::new();
            for i in sinked_count..sinked_count + batch_size {
                messages.push(&data[i]);
            }

            self.invoke(messages).await.unwrap();
            sinked_count += batch_size;
            if sinked_count == all_count {
                break;
            }
        }
        Ok(())
    }

    async fn sink_ddl(&mut self, _data: Vec<DdlData>, _batch: bool) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl OpenFaasSinker {
    async fn invoke(&self, data: Vec<&RowData>) -> Result<(), Error> {
        let response = self
            .http_client
            .post(&self.gateway_url)
            .header("Content-Type", "application/json")
            .body(json!(data).to_string())
            .send()
            .await
            .unwrap();
        if !response.status().is_success() {
            error!(
                "invoke open faas failed, error: {}",
                response.text().await.unwrap()
            );
        }
        Ok(())
    }
}
