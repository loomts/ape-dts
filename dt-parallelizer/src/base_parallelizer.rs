use anyhow::bail;
use dt_common::meta::{ddl_data::DdlData, dt_data::DtItem, dt_queue::DtQueue, row_data::RowData};
use dt_common::monitor::counter::Counter;
use dt_common::monitor::counter_type::CounterType;
use dt_common::{error::Error, monitor::monitor::Monitor};
use dt_connector::Sinker;
use ratelimit::Ratelimiter;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

pub struct BaseParallelizer {
    pub poped_data: VecDeque<DtItem>,
    pub monitor: Arc<Mutex<Monitor>>,
    pub rps_limiter: Option<Ratelimiter>,
}

impl BaseParallelizer {
    pub async fn drain(&mut self, buffer: &DtQueue) -> anyhow::Result<Vec<DtItem>> {
        let mut data = Vec::new();
        while let Some(item) = self.poped_data.pop_front() {
            data.push(item);
        }

        let mut record_size_counter = Counter::new(0, 0);
        // ddls and dmls should be drained seperately
        while let Ok(item) = self.pop(buffer, &mut record_size_counter).await {
            if data.is_empty()
                || (data[0].is_ddl() == item.is_ddl()
                    && data[0].data_origin_node == item.data_origin_node)
            {
                data.push(item);
            } else {
                self.poped_data.push_back(item);
                break;
            }
        }

        self.update_monitor(&record_size_counter).await;
        Ok(data)
    }

    pub async fn pop(
        &self,
        buffer: &DtQueue,
        record_size_counter: &mut Counter,
    ) -> anyhow::Result<DtItem> {
        // rps limit
        if let Some(rps_limiter) = &self.rps_limiter {
            // refer: https://docs.rs/ratelimit/0.7.1/ratelimit
            if let Err(_sleep) = rps_limiter.try_wait() {
                bail! {Error::PipelineError(format!(
                    "reach rps limit: {}",
                    rps_limiter.max_tokens(),
                ))};
            }
        }

        match buffer.pop() {
            Ok(item) => {
                // counter
                record_size_counter
                    .add(item.dt_data.get_data_size(), item.dt_data.get_data_count());
                Ok(item)
            }
            Err(error) => bail! {Error::PipelineError(format!("buffer pop error: {}", error))},
        }
    }

    pub async fn update_monitor(&self, record_size_counter: &Counter) {
        if record_size_counter.value > 0 {
            self.monitor.lock().unwrap().add_batch_counter(
                CounterType::RecordSize,
                record_size_counter.value,
                record_size_counter.count,
            );
        }
    }

    pub async fn sink_dml(
        &self,
        mut sub_datas: Vec<Vec<RowData>>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
        parallel_size: usize,
        batch: bool,
    ) -> anyhow::Result<()> {
        let mut futures = Vec::new();
        for i in 0..sub_datas.len() {
            let data = sub_datas.remove(0);
            let sinker = sinkers[i % parallel_size].clone();
            let future =
                tokio::spawn(
                    async move { sinker.lock().await.sink_dml(data, batch).await.unwrap() },
                );
            futures.push(future);
        }

        for future in futures {
            future.await.unwrap();
        }
        Ok(())
    }

    pub async fn sink_ddl(
        &self,
        mut sub_datas: Vec<Vec<DdlData>>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
        parallel_size: usize,
        batch: bool,
    ) -> anyhow::Result<()> {
        let mut futures = Vec::new();
        for i in 0..sub_datas.len() {
            let data = sub_datas.remove(0);
            let sinker = sinkers[i % parallel_size].clone();
            let future =
                tokio::spawn(
                    async move { sinker.lock().await.sink_ddl(data, batch).await.unwrap() },
                );
            futures.push(future);
        }

        for future in futures {
            future.await.unwrap();
        }
        Ok(())
    }

    pub async fn sink_raw(
        &self,
        mut sub_datas: Vec<Vec<DtItem>>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
        parallel_size: usize,
        batch: bool,
    ) -> anyhow::Result<()> {
        let mut futures = Vec::new();
        for i in 0..sub_datas.len() {
            let data = sub_datas.remove(0);
            let sinker = sinkers[i % parallel_size].clone();
            let future =
                tokio::spawn(
                    async move { sinker.lock().await.sink_raw(data, batch).await.unwrap() },
                );
            futures.push(future);
        }

        for future in futures {
            future.await.unwrap();
        }
        Ok(())
    }
}
