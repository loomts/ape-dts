use super::config_enums::ParallelType;

#[derive(Clone)]
pub struct ParallelizerConfig {
    pub parallel_type: ParallelType,
    pub parallel_size: usize,
}
