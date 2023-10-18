use std::sync::{atomic::AtomicBool, Arc, Mutex};

use crate::{
    avro::avro_converter::AvroConverter, extractor::base_extractor::BaseExtractor, Extractor,
};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::{error::Error, log_info, syncer::Syncer};
use dt_meta::dt_data::DtData;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig, Message, Offset, TopicPartitionList,
};

pub struct KafkaExtractor {
    pub buffer: Arc<ConcurrentQueue<DtData>>,
    pub shut_down: Arc<AtomicBool>,
    pub url: String,
    pub group: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub ack_interval_secs: u64,
    pub avro_converter: AvroConverter,
    pub syncer: Arc<Mutex<Syncer>>,
}

#[async_trait]
impl Extractor for KafkaExtractor {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!("KafkaCdcExtractor starts");
        let consumer = self.create_consumer();
        self.extract_avro(consumer).await
    }
}

impl KafkaExtractor {
    async fn extract(&mut self, consumer: StreamConsumer) -> Result<(), Error> {
        loop {
            let msg = consumer.recv().await.unwrap();
            let msg_position = format!("offset:{}", msg.offset());
            if let Some(payload) = msg.payload() {
                let mut dt_data: DtData = serde_json::from_slice(payload).unwrap();
                match &mut dt_data {
                    DtData::Commit { position, .. } => *position = msg_position,
                    DtData::Dml { row_data } => row_data.position = msg_position,
                    _ => {}
                };
                BaseExtractor::push_dt_data(&self.buffer, dt_data).await?;
            }
        }
    }

    async fn extract_avro(&mut self, consumer: StreamConsumer) -> Result<(), Error> {
        loop {
            let msg = consumer.recv().await.unwrap();
            if let Some(payload) = msg.payload() {
                let mut row_data = self
                    .avro_converter
                    .avro_value_to_row_data(payload.to_vec())
                    .unwrap();
                row_data.position = format!("offset:{}", msg.offset());
                BaseExtractor::push_row(&self.buffer, row_data).await?;
            }
        }
    }

    fn create_consumer(&self) -> StreamConsumer {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.url);
        config.set("group.id", &self.group);
        config.set("auto.offset.reset", "latest");
        config.set("session.timeout.ms", "10000");

        let consumer: StreamConsumer = config.create().unwrap();
        // only support extract data from one topic, one partition
        let mut tpl = TopicPartitionList::new();
        if self.offset >= 0 {
            tpl.add_partition_offset(&self.topic, self.partition, Offset::Offset(self.offset))
                .unwrap();
        } else {
            tpl.add_partition(&self.topic, self.partition);
        }
        consumer.assign(&tpl).unwrap();
        consumer
    }
}
