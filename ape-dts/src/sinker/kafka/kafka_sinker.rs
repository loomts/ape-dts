use async_trait::async_trait;

use crate::{
    call_batch_fn,
    error::Error,
    meta::{ddl_data::DdlData, row_data::RowData},
    traits::Sinker,
};

use kafka::producer::{Producer, Record};

use super::kafka_router::KafkaRouter;

pub struct KafkaSinker {
    pub batch_size: usize,
    pub kafka_router: KafkaRouter,
    pub producer: Producer,
}

#[async_trait]
impl Sinker for KafkaSinker {
    async fn sink_dml(&mut self, mut data: Vec<RowData>, _batch: bool) -> Result<(), Error> {
        call_batch_fn!(self, data, Self::send);
        Ok(())
    }

    async fn sink_ddl(&mut self, _data: Vec<DdlData>, _batch: bool) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl KafkaSinker {
    async fn send(
        &mut self,
        data: &mut Vec<RowData>,
        sinked_count: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let mut topics = Vec::new();
        for i in sinked_count..sinked_count + batch_size {
            let row_data = &data[i];
            let topic = self.kafka_router.get_route(&row_data.schema, &row_data.tb);
            topics.push(topic);
        }

        let mut messages = Vec::new();
        for i in sinked_count..sinked_count + batch_size {
            let row_data = &data[i];
            messages.push(Record {
                key: (),
                value: row_data.to_string(),
                topic: &topics[i - sinked_count],
                partition: -1,
            });
        }

        self.producer.send_all(&messages).unwrap();
        Ok(())
    }
}
