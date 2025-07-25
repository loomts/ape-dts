#[cfg(feature = "metrics")]
use std::collections::HashMap;

#[derive(Clone)]
#[cfg(feature = "metrics")]
pub struct MetricsConfig {
    pub http_host: String,
    pub http_port: u64,
    pub workers: u64,
    // come from task config such like: k1=v1,k2=v2
    pub metrics_labels: HashMap<String, String>,
}
