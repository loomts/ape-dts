pub struct PipelineConfig {
    // 1, parallel, 2, rdb_merge
    pub pipeline_type: PipelineType,
    pub parallel_size: usize,
    pub buffer_size: usize,
    pub checkpoint_interval_secs: u64,
}

pub enum PipelineType {
    DEFAULT,
    MERGE,
    SNAPSHOT,
}

impl PipelineType {
    pub fn from_str(type_str: &str) -> Self {
        match type_str {
            "merge" => Self::MERGE,
            "snapshot" => Self::SNAPSHOT,
            _ => Self::DEFAULT,
        }
    }
}
