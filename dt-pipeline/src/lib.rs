pub mod base_pipeline;

use std::sync::Arc;

use async_rwlock::RwLock;
use async_trait::async_trait;
use dt_common::{error::Error, monitor::monitor::Monitor};

#[async_trait]
pub trait Pipeline {
    async fn start(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn get_monitor(&self) -> Option<Arc<RwLock<Monitor>>> {
        None
    }
}
