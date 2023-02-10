use async_trait::async_trait;

use crate::error::Error;

#[async_trait]
pub trait Extractor {
    async fn extract(&mut self) -> Result<(), Error>;
}
