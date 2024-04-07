use async_trait::async_trait;

use crate::Sinker;

pub struct DummySinker {}

#[async_trait]
impl Sinker for DummySinker {}
