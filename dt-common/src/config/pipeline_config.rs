#[derive(Clone)]
pub struct PipelineConfig {
    pub buffer_size: usize,
    pub buffer_memory_mb: usize,
    pub max_rps: u64,
    pub checkpoint_interval_secs: u64,
    pub batch_sink_interval_secs: u64,
}
