use std::time::Duration;

use concurrent_queue::ConcurrentQueue;
use dt_common::error::Error;

use crate::meta::common::database_model::StructModel;

pub struct QueueOperator {}

impl QueueOperator {
    pub async fn push_to_queue(
        struct_obj_queue: &ConcurrentQueue<StructModel>,
        model: crate::StructModel,
        millis: u64,
    ) -> Result<(), Error> {
        while struct_obj_queue.is_full() {
            tokio::time::sleep(Duration::from_millis(millis)).await;
        }
        let _ = struct_obj_queue.push(model);
        return Ok(());
    }
}
