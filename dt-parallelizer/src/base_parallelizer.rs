use std::collections::VecDeque;
use std::sync::Arc;

use async_rwlock::RwLock;
use concurrent_queue::ConcurrentQueue;
use dt_common::monitor::counter::Counter;
use dt_common::monitor::monitor::CounterType;
use dt_common::{error::Error, monitor::monitor::Monitor};
use dt_connector::Sinker;
use dt_meta::{
    ddl_data::DdlData,
    dt_data::{DtData, DtItem},
    row_data::RowData,
};
use ratelimit::Ratelimiter;

pub struct BaseParallelizer {
    pub poped_data: VecDeque<DtItem>,
    pub monitor: Arc<RwLock<Monitor>>,
    pub rps_limiter: Option<Ratelimiter>,
}

impl BaseParallelizer {
    pub async fn drain(&mut self, buffer: &ConcurrentQueue<DtItem>) -> Result<Vec<DtItem>, Error> {
        let mut data = Vec::new();
        while let Some(item) = self.poped_data.pop_front() {
            data.push(item);
        }

        let mut record_size_counter = Counter::new(0, 0);
        // ddls and dmls should be drained seperately
        while let Ok(item) = self.pop(buffer, &mut record_size_counter) {
            if data.is_empty() || data[0].is_ddl() == item.is_ddl() {
                data.push(item);
            } else {
                self.poped_data.push_back(item);
                break;
            }
        }

        self.update_monitor(&record_size_counter).await;
        Ok(data)
    }

    pub fn pop(
        &self,
        buffer: &ConcurrentQueue<DtItem>,
        record_size_counter: &mut Counter,
    ) -> Result<DtItem, Error> {
        // rps limit
        if let Some(rps_limiter) = &self.rps_limiter {
            // refer: https://docs.rs/ratelimit/0.7.1/ratelimit
            if let Err(_sleep) = rps_limiter.try_wait() {
                return Err(Error::PipelineError(format!(
                    "reach rps limit: {}",
                    rps_limiter.max_tokens(),
                )));
            }
        }

        match buffer.pop() {
            Ok(item) => {
                // counter
                record_size_counter.add(item.get_data_malloc_size(), 1);
                Ok(item)
            }
            Err(error) => Err(Error::PipelineError(format!(
                "buffer pop error: {}",
                error.to_string()
            ))),
        }
    }

    pub async fn update_monitor(&self, record_size_counter: &Counter) {
        if record_size_counter.value > 0 {
            self.monitor.write().await.add_batch_counter(
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
    ) -> Result<(), Error> {
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
    ) -> Result<(), Error> {
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
        mut sub_datas: Vec<Vec<DtData>>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
        parallel_size: usize,
        batch: bool,
    ) -> Result<(), Error> {
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
