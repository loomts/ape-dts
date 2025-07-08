use std::sync::Arc;

use tokio::{sync::Mutex, time::Instant};

use dt_common::monitor::{counter_type::CounterType, monitor::Monitor};

pub struct BaseSinker {}

impl BaseSinker {
    pub async fn update_batch_monitor(
        monitor: &mut Arc<Mutex<Monitor>>,
        batch_size: usize,
        data_size: usize,
        start_time: Instant,
    ) -> anyhow::Result<()> {
        monitor
            .lock()
            .await
            .add_counter(CounterType::RecordsPerQuery, batch_size)
            .add_counter(CounterType::RecordCount, batch_size)
            .add_counter(CounterType::DataBytes, data_size)
            .add_counter(
                CounterType::RtPerQuery,
                start_time.elapsed().as_micros() as usize,
            );
        Ok(())
    }

    pub async fn update_serial_monitor(
        monitor: &mut Arc<Mutex<Monitor>>,
        record_count: usize,
        data_size: usize,
        start_time: Instant,
    ) -> anyhow::Result<()> {
        monitor
            .lock()
            .await
            .add_batch_counter(CounterType::RecordsPerQuery, record_count, record_count)
            .add_counter(CounterType::RecordCount, record_count)
            .add_counter(CounterType::SerialWrites, record_count)
            .add_batch_counter(CounterType::DataBytes, data_size, record_count)
            .add_batch_counter(
                CounterType::RtPerQuery,
                start_time.elapsed().as_micros() as usize,
                record_count,
            );
        Ok(())
    }
}

#[macro_export(local_inner_macros)]
macro_rules! call_batch_fn {
    ($self:ident, $data:ident, $batch_fn:expr) => {
        let all_count = $data.len();
        let mut sinked_count = 0;

        loop {
            let mut batch_size = $self.batch_size;
            if all_count - sinked_count < batch_size {
                batch_size = all_count - sinked_count;
            }

            if batch_size == 0 {
                break;
            }

            $batch_fn($self, &mut $data, sinked_count, batch_size).await?;
            sinked_count += batch_size;
        }
    };
}

#[macro_export(local_inner_macros)]
macro_rules! sync_call_batch_fn {
    ($self:ident, $data:ident, $batch_fn:expr) => {
        let all_count = $data.len();
        let mut sinked_count = 0;

        loop {
            let mut batch_size = $self.batch_size;
            if all_count - sinked_count < batch_size {
                batch_size = all_count - sinked_count;
            }

            if batch_size == 0 {
                break;
            }

            $batch_fn($self, &mut $data, sinked_count, batch_size)?;
            sinked_count += batch_size;
        }
    };
}
