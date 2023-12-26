use std::{
    sync::{atomic::AtomicBool, Arc, Mutex},
    time::Instant,
};

use crate::{extractor::base_extractor::BaseExtractor, Extractor};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::{error::Error, log_info, monitor::monitor::Monitor};
use dt_meta::{
    avro::avro_converter::AvroConverter, dt_data::DtItem, position::Position, syncer::Syncer,
};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig, Message, Offset, TopicPartitionList,
};

pub struct KafkaExtractor {
    pub buffer: Arc<ConcurrentQueue<DtItem>>,
    pub shut_down: Arc<AtomicBool>,
    pub url: String,
    pub group: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub ack_interval_secs: u64,
    pub avro_converter: AvroConverter,
    pub syncer: Arc<Mutex<Syncer>>,
    pub monitor: Arc<Mutex<Monitor>>,
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
    async fn extract_avro(&mut self, consumer: StreamConsumer) -> Result<(), Error> {
        let mut last_monitored_time = Instant::now();
        let monitor_count_window = self.monitor.lock().unwrap().count_window;
        let monitor_time_window_secs = self.monitor.lock().unwrap().time_window_secs as u64;
        let mut monitored_count = 0;
        let mut extracted_count = 0;

        loop {
            let msg = consumer.recv().await.unwrap();
            if let Some(payload) = msg.payload() {
                let row_data = self
                    .avro_converter
                    .avro_value_to_row_data(payload.to_vec())
                    .unwrap();
                let position = Position::Kafka {
                    topic: self.topic.clone(),
                    partition: self.partition,
                    offset: msg.offset(),
                };
                BaseExtractor::push_row(&self.buffer, row_data, position, None).await?;
                extracted_count += 1;
            }

            (last_monitored_time, monitored_count) = BaseExtractor::update_monitor(
                &mut self.monitor,
                extracted_count,
                monitored_count,
                monitor_count_window,
                monitor_time_window_secs,
                last_monitored_time,
            );
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
