use async_trait::async_trait;

use crate::{call_batch_fn, Sinker};

use dt_common::error::Error;

use dt_meta::dt_data::DtData;

use kafka::producer::{Producer, Record};

use super::kafka_router::KafkaRouter;

pub struct KafkaSinker {
    pub batch_size: usize,
    pub kafka_router: KafkaRouter,
    pub producer: Producer,
}

#[async_trait]
impl Sinker for KafkaSinker {
    async fn sink_raw(&mut self, mut data: Vec<DtData>, _batch: bool) -> Result<(), Error> {
        call_batch_fn!(self, data, Self::send);
        Ok(())
    }
}

impl KafkaSinker {
    async fn send(
        &mut self,
        data: &mut [DtData],
        sinked_count: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        // let mut topics = Vec::new();
        // for rd in data.iter().skip(sinked_count).take(batch_size) {
        //     let topic = self.kafka_router.get_route(&rd.schema, &rd.tb);
        //     topics.push(topic);
        // }

        let topic = self.kafka_router.get_route("", "");

        let mut messages = Vec::new();
        for (_, dt_data) in data.iter().skip(sinked_count).take(batch_size).enumerate() {
            messages.push(Record {
                key: (),
                value: dt_data.to_string(),
                topic: &topic,
                partition: -1,
            });
        }

        self.producer.send_all(&messages).unwrap();
        Ok(())
    }
}
