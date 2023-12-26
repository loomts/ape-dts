use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use async_trait::async_trait;

use crate::{call_batch_fn, rdb_router::RdbRouter, sinker::base_sinker::BaseSinker, Sinker};

use dt_common::{error::Error, monitor::monitor::Monitor};

use dt_meta::{avro::avro_converter::AvroConverter, row_data::RowData};

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
    async fn sink_dml(&mut self, mut data: Vec<RowData>, _batch: bool) -> Result<(), Error> {
        let start_time = Instant::now();

        call_batch_fn!(self, data, Self::send_avro);

        BaseSinker::update_batch_monitor(&mut self.monitor, data.len(), start_time).await
    }
}

impl KafkaSinker {
    async fn send_avro(
        &mut self,
        data: &mut [RowData],
        sinked_count: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let mut messages = Vec::new();
        for (_, row_data) in data
            .iter_mut()
            .skip(sinked_count)
            .take(batch_size)
            .enumerate()
        {
            let topic = self.router.get_topic(&row_data.schema, &row_data.tb);
            let key = self.avro_converter.row_data_to_avro_key(row_data).await?;
            let payload = self
                .avro_converter
                .row_data_to_avro_value(row_data.clone())
                .unwrap();
            messages.push(Record {
                key,
                value: payload,
                topic,
                partition: -1,
            });
        }

        self.producer.send_all(&messages).unwrap();
        Ok(())
    }
}
