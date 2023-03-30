use std::sync::Arc;

use concurrent_queue::ConcurrentQueue;

use crate::meta::row_data::RowData;
use crate::{error::Error, meta::dt_data::DtData, traits::Sinker};

pub struct ParallelizerUtil {}

impl ParallelizerUtil {
    pub fn drain(buffer: &ConcurrentQueue<DtData>) -> Result<Vec<DtData>, Error> {
        let mut data = Vec::new();
        while let Ok(dt_data) = buffer.pop() {
            data.push(dt_data);
        }
        Ok(data)
    }

    pub async fn sink(
        mut sub_datas: Vec<Vec<RowData>>,
        sinkers: &Vec<Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>>,
        parallel_size: usize,
        sink_batch: bool,
    ) -> Result<(), Error> {
        let mut futures = Vec::new();
        for i in 0..sub_datas.len() {
            let data = sub_datas.remove(0);
            let sinker = sinkers[i % parallel_size].clone();
            let future = tokio::spawn(async move {
                if sink_batch {
                    sinker.lock().await.batch_sink(data).await.unwrap()
                } else {
                    sinker.lock().await.sink(data).await.unwrap()
                }
            });
            futures.push(future);
        }

        for future in futures {
            future.await.unwrap();
        }
        Ok(())
    }
}
