use super::config_enums::ParallelType;

pub struct PipelineConfig {
    pub parallel_type: ParallelType,
    pub parallel_size: usize,
    pub buffer_size: usize,
    pub checkpoint_interval_secs: u64,
}
