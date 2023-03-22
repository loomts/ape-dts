use crate::error::Error;

pub struct PipelineConfig {
    // 1, parallel, 2, rdb_merge
    pub pipeline_type: PipelineType,
    pub parallel_size: usize,
    pub buffer_size: usize,
    pub checkpoint_interval_secs: u64,
}

pub enum PipelineType {
    Snapshot,
    RdbPartition,
    RdbMerge,
    RdbCheck,
}

impl PipelineType {
    pub fn to_string(&self) -> String {
        match self {
            Self::Snapshot => "snapshot".to_string(),
            Self::RdbMerge => "rdb_merge".to_string(),
            Self::RdbCheck => "rdb_check".to_string(),
            Self::RdbPartition => "rdb_partition".to_string(),
        }
    }

    pub fn from_str(type_str: &str) -> Result<Self, Error> {
        match type_str {
            "snapshot" => Ok(Self::Snapshot),
            "rdb_merge" => Ok(Self::RdbMerge),
            "rdb_check" => Ok(Self::RdbCheck),
            "rdb_partition" => Ok(Self::RdbPartition),
            _ => Err(Error::Unexpected {
                error: "unexpected sinker type".to_string(),
            }),
        }
    }
}
