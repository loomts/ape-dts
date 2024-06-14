use std::sync::atomic::{AtomicI64, Ordering};

use concurrent_queue::{ConcurrentQueue, PopError};

use crate::utils::time_util::TimeUtil;

use super::dt_data::DtItem;

pub struct DtQueue {
    queue: ConcurrentQueue<DtItem>,
    check_memory: bool,
    max_bytes: i64,
    cur_bytes: AtomicI64,
}

impl DtQueue {
    pub fn new(capacity: usize, max_bytes: i64) -> Self {
        Self {
            queue: ConcurrentQueue::bounded(capacity),
            max_bytes,
            check_memory: max_bytes > 0,
            cur_bytes: AtomicI64::new(0),
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.queue.is_full()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    #[inline(always)]
    pub async fn push(&self, item: DtItem) -> anyhow::Result<()> {
        while self.queue.is_full() {
            TimeUtil::sleep_millis(1).await;
        }

        if self.check_memory {
            while self.cur_bytes.load(Ordering::Acquire) > self.max_bytes {
                TimeUtil::sleep_millis(1).await;
            }

            self.cur_bytes
                .fetch_add(item.dt_data.get_data_size() as i64, Ordering::Release);
        }

        self.queue.push(item)?;
        Ok(())
    }

    #[inline(always)]
    pub fn pop(&self) -> anyhow::Result<DtItem, PopError> {
        let item = self.queue.pop()?;

        if self.check_memory {
            if self.queue.is_empty() {
                self.cur_bytes.store(0, Ordering::Release);
            } else {
                self.cur_bytes
                    .fetch_sub(item.dt_data.get_data_size() as i64, Ordering::Release);
            }
        }

        Ok(item)
    }
}
