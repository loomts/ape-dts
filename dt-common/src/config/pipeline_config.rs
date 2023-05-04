use super::config_enums::ParallelType;

#[derive(Clone)]
pub struct PipelineConfig {
    pub parallel_type: ParallelType,
    pub parallel_size: usize,
    pub buffer_size: usize,
    pub checkpoint_interval_secs: u64,
    pub batch_sink_interval_secs: u64,
}
