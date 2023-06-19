use crate::{call_batch_fn, Sinker};
use async_trait::async_trait;
use dt_common::{error::Error, log_error};
use dt_meta::{ddl_data::DdlData, row_data::RowData};
use reqwest::Client;
use serde_json::json;

pub struct OpenFaasSinker {
    pub batch_size: usize,
    pub gateway_url: String,
    pub http_client: Client,
}

#[async_trait]
impl Sinker for OpenFaasSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, _batch: bool) -> Result<(), Error> {
        call_batch_fn!(self, data, Self::invoke);
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
    async fn invoke(
        &mut self,
        data: &mut Vec<RowData>,
        start_index: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let mut messages = Vec::new();
        for rd in data.iter().skip(start_index).take(batch_size) {
            messages.push(rd);
        }

        let response = self
            .http_client
            .post(&self.gateway_url)
            .header("Content-Type", "application/json")
            .body(json!(data).to_string())
            .send()
            .await
            .unwrap();
        if !response.status().is_success() {
            log_error!(
                "invoke open faas failed, error: {}",
                response.text().await.unwrap()
            );
        }
        Ok(())
    }
}
