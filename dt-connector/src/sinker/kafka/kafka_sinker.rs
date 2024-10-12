use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use async_trait::async_trait;

use crate::{call_batch_fn, rdb_router::RdbRouter, sinker::base_sinker::BaseSinker, Sinker};

use dt_common::{meta::ddl_meta::ddl_data::DdlData, monitor::monitor::Monitor};

use dt_common::meta::{avro::avro_converter::AvroConverter, row_data::RowData};

use kafka::producer::{Producer, Record};

pub struct KafkaSinker {
    pub batch_size: usize,
    pub router: RdbRouter,
    pub producer: Producer,
    pub avro_converter: AvroConverter,
    pub monitor: Arc<Mutex<Monitor>>,
}

#[async_trait]
impl Sinker for KafkaSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, _batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        call_batch_fn!(self, data, Self::send_avro);
        Ok(())
    }

    async fn sink_ddl(&mut self, data: Vec<DdlData>, _batch: bool) -> anyhow::Result<()> {
        let mut messages = Vec::new();
        for ddl_data in data {
            let topic = self.router.get_topic(&ddl_data.default_schema, "");
            let payload = self.avro_converter.ddl_data_to_avro_value(ddl_data).await?;
            messages.push(Record {
                key: String::new(),
                value: payload,
                topic,
                partition: -1,
            });
        }
        self.producer.send_all(&messages)?;
        Ok(())
    }

    async fn refresh_meta(&mut self, data: Vec<DdlData>) -> anyhow::Result<()> {
        self.avro_converter.refresh_meta(&data);
        Ok(())
    }
}

impl KafkaSinker {
    async fn send_avro(
        &mut self,
        data: &mut [RowData],
        sinked_count: usize,
        batch_size: usize,
    ) -> anyhow::Result<()> {
        let start_time = Instant::now();
        let mut data_size = 0;

        let mut messages = Vec::new();
        for row_data in data.iter_mut().skip(sinked_count).take(batch_size) {
            data_size += row_data.data_size;

            row_data.convert_raw_string();
            let topic = self.router.get_topic(&row_data.schema, &row_data.tb);
            let key = self.avro_converter.row_data_to_avro_key(row_data).await?;
            let payload = self
                .avro_converter
                .row_data_to_avro_value(row_data.clone())
                .await?;
            messages.push(Record {
                key,
                value: payload,
                topic,
                partition: -1,
            });
        }

        self.producer.send_all(&messages)?;

        BaseSinker::update_batch_monitor(&mut self.monitor, batch_size, data_size, start_time).await
    }
}
