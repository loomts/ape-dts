use async_trait::async_trait;

use crate::{call_batch_fn, rdb_router::RdbRouter, Sinker};

use dt_common::error::Error;

use dt_meta::{avro::avro_converter::AvroConverter, row_data::RowData};

use kafka::producer::{Producer, Record};

pub struct KafkaSinker {
    pub batch_size: usize,
    pub router: RdbRouter,
    pub producer: Producer,
    pub avro_converter: AvroConverter,
}

#[async_trait]
impl Sinker for KafkaSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, _batch: bool) -> Result<(), Error> {
        call_batch_fn!(self, data, Self::send_avro);
        Ok(())
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
