use async_trait::async_trait;
use dt_common::error::Error;

pub mod base_pipeline;
pub mod filters;
pub mod transaction_pipeline;
pub mod utils;

#[async_trait]
pub trait Pipeline {
    async fn stop(&mut self) -> Result<(), Error>;

    async fn start(&mut self) -> Result<(), Error>;
}
