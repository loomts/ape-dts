use std::{
    cmp,
    collections::HashMap,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Instant,
};

use crate::{call_batch_fn, sinker::base_sinker::BaseSinker, Sinker};
use anyhow::bail;
use async_trait::async_trait;
use chrono::Utc;
use dt_common::{
    error::Error,
    log_error,
    meta::mysql::{
        mysql_col_type::MysqlColType, mysql_meta_manager::MysqlMetaManager,
        mysql_tb_meta::MysqlTbMeta,
    },
    monitor::monitor::Monitor,
};
use dt_common::{
    meta::{col_value::ColValue, row_data::RowData, row_type::RowType},
    utils::sql_util::SqlUtil,
};
use reqwest::{header, Client, Method, Response, StatusCode};
use serde_json::{json, Value};

const SIGN_COL_NAME: &str = "_ape_dts_is_deleted";
const VERSION_COL_NAME: &str = "_ape_dts_version";

#[derive(Clone)]
pub struct StarRocksSinker {
    pub batch_size: usize,
    pub http_client: Client,
    pub host: String,
    pub port: String,
    pub username: String,
    pub password: String,
    pub meta_manager: MysqlMetaManager,
    pub monitor: Arc<Mutex<Monitor>>,
    pub sync_version: i64,
    pub logical_delete: bool,
}

#[async_trait]
impl Sinker for StarRocksSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        if !batch {
            self.serial_sink(data).await?;
        } else {
            match data[0].row_type {
                RowType::Insert | RowType::Delete => {
                    call_batch_fn!(self, data, Self::batch_sink);
                }
                _ => self.serial_sink(data).await?,
            }
        }
        Ok(())
    }
}

impl StarRocksSinker {
    async fn serial_sink(&mut self, mut data: Vec<RowData>) -> anyhow::Result<()> {
        let start_time = Instant::now();
        let mut data_size = 0;

        let data = data.as_mut_slice();
        for i in 0..data.len() {
            data_size += data[i].data_size;
            self.send_data(data, i, 1).await?;
        }

        BaseSinker::update_serial_monitor(&mut self.monitor, data.len(), data_size, start_time)
            .await
    }

    async fn batch_sink(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let start_time = Instant::now();

        let data_size = self.send_data(data, start_index, batch_size).await?;

        BaseSinker::update_batch_monitor(&mut self.monitor, batch_size, data_size, start_time).await
    }

    async fn send_data(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> anyhow::Result<usize> {
        let db = data[start_index].schema.clone();
        let tb = data[start_index].tb.clone();
        let row_type = data[start_index].row_type.clone();
        let tb_meta = self.meta_manager.get_tb_meta(&db, &tb).await?;

        // use timestamp_millis as VERSION_COL value
        self.sync_version = cmp::max(Utc::now().timestamp_millis(), self.sync_version + 1);

        let mut data_size = 0;
        // build stream load data
        let mut load_data = Vec::new();
        for row_data in data.iter_mut().skip(start_index).take(batch_size) {
            data_size += row_data.data_size;

            Self::convert_row_data(row_data, tb_meta)?;

            let col_values = if row_type == RowType::Delete {
                let before = row_data.before.as_mut().unwrap();
                // SIGN_COL value
                before.insert(SIGN_COL_NAME.into(), ColValue::Long(1));
                before
            } else {
                row_data.after.as_mut().unwrap()
            };

            // VERSION_COL value
            col_values.insert(
                VERSION_COL_NAME.into(),
                ColValue::LongLong(self.sync_version),
            );
            load_data.push(col_values);
        }

        let logical_delete = self.logical_delete
            && tb_meta
                .basic
                .col_origin_type_map
                .contains_key(SIGN_COL_NAME);

        let mut op = "";
        if row_type == RowType::Delete && !logical_delete {
            op = "delete";
        }

        let body = json!(load_data).to_string();
        // do stream load
        let url = format!(
            "http://{}:{}/api/{}/{}/_stream_load",
            self.host, self.port, db, tb
        );
        let request = self.build_request(&url, op, &body)?;
        let response = self.http_client.execute(request).await?;
        Self::check_response(response).await?;

        Ok(data_size)
    }

    fn convert_row_data(row_data: &mut RowData, tb_meta: &MysqlTbMeta) -> anyhow::Result<()> {
        if let Some(before) = &mut row_data.before {
            Self::convert_col_values(before, tb_meta)?;
        }
        if let Some(after) = &mut row_data.after {
            Self::convert_col_values(after, tb_meta)?;
        }
        Ok(())
    }

    fn convert_col_values(
        col_values: &mut HashMap<String, ColValue>,
        tb_meta: &MysqlTbMeta,
    ) -> anyhow::Result<()> {
        let mut new_col_values: HashMap<String, ColValue> = HashMap::new();
        for (col, col_value) in col_values.iter() {
            if let MysqlColType::Json = tb_meta.get_col_type(col)? {
                // ColValue::Json2 will be serialized to:
                // {"id": 1, "json_field": "{\"name\": \"Alice\", \"age\": 30}"}
                // ColValue::Json3 will be serialized to:
                // {"id": 5, "json_field": {"name": "Alice", "age": 30}}
                if let ColValue::Json2(v) = col_value {
                    if let Ok(json_v) = serde_json::Value::from_str(v) {
                        new_col_values.insert(col.to_owned(), ColValue::Json3(json_v));
                    }
                }
            }

            match col_value {
                ColValue::Blob(v) | ColValue::RawString(v) => {
                    new_col_values.insert(
                        col.to_owned(),
                        ColValue::String(SqlUtil::binary_to_str(v).0),
                    );
                }

                _ => {}
            }
        }

        for (col, col_value) in new_col_values {
            col_values.insert(col, col_value);
        }
        Ok(())
    }

    fn build_request(&self, url: &str, op: &str, body: &str) -> anyhow::Result<reqwest::Request> {
        let password = if self.password.is_empty() {
            None
        } else {
            Some(self.password.clone())
        };

        // https://docs.starrocks.io/en-us/2.5/loading/Load_to_Primary_Key_tables
        let mut put = self
            .http_client
            .request(Method::PUT, url)
            .basic_auth(&self.username, password)
            .header(header::EXPECT, "100-continue")
            .header("format", "json")
            .header("strip_outer_array", "true")
            .body(body.to_string());
        // by default, the __op will be upsert
        if !op.is_empty() {
            let op = format!("__op='{}'", op);
            put = put.header("columns", op);
        }
        Ok(put.build()?)
    }

    async fn check_response(response: Response) -> anyhow::Result<()> {
        let status_code = response.status();
        let response_text = &response.text().await?;
        if status_code != StatusCode::OK {
            bail! {Error::HttpError(format!(
                "data load request failed, status_code: {}, response_text: {:?}",
                status_code, response_text
            ))}
        }

        // response example:
        // {
        //     "TxnId": 2039,
        //     "Label": "54afc14e-9088-46df-b532-4deaf4437b42",
        //     "Status": "Success",
        //     "Message": "OK",
        //     "NumberTotalRows": 3,
        //     "NumberLoadedRows": 3,
        //     "NumberFilteredRows": 0,
        //     "NumberUnselectedRows": 0,
        //     "LoadBytes": 221,
        //     "LoadTimeMs": 228,
        //     "BeginTxnTimeMs": 34,
        //     "StreamLoadPlanTimeMs": 48,
        //     "ReadDataTimeMs": 0,
        //     "WriteDataTimeMs": 107,
        //     "CommitAndPublishTimeMs": 36
        // }
        let json_value: Value = serde_json::from_str(response_text)?;
        if json_value["Status"] != "Success" {
            let err = format!(
                "stream load request failed, status_code: {}, load_result: {}",
                status_code, response_text,
            );
            log_error!("{}", err);
            bail! {Error::HttpError(err)}
        }
        Ok(())
    }
}
