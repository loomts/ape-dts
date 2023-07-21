use core::fmt;

use super::config_enums::PipelineType;

#[derive(Clone)]
pub struct PipelineConfig {
    pub buffer_size: usize,
    pub checkpoint_interval_secs: u64,
    pub batch_sink_interval_secs: u64,

    pub extra_config: ExtraConfig,
}

#[derive(Clone, Debug)]
pub enum ExtraConfig {
    Basic {},
    Transaction {
        transaction_db: String,
        transaction_table: String,
        transaction_express: String,
        transaction_command: String,
        white_nodes: String,
        black_nodes: String,
    },
}

impl PipelineConfig {
    pub fn get_pipeline_type(&self) -> PipelineType {
        match self.extra_config {
            ExtraConfig::Transaction { .. } => PipelineType::Transaction,
            _ => PipelineType::Basic,
        }
    }
}

impl fmt::Display for PipelineConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.extra_config {
            ExtraConfig::Transaction {
                transaction_db,
                transaction_table,
                transaction_express,
                transaction_command,
                white_nodes,
                black_nodes,
            } => {
                write!(f, "PipelineConfig:{{buffer_size:{},checkpoint_interval_secs:{},batch_sink_interval_secs:{},transaction_db:{}, transaction_table:{},transaction_express:{}, transaction_command:{}, white_nodes:{},black_nodes:{}}}", self.buffer_size, self.checkpoint_interval_secs, self.batch_sink_interval_secs, transaction_db, transaction_table, transaction_express, transaction_command, white_nodes, black_nodes)
            }
            _ => {
                write!(f, "PipelineConfig:{{buffer_size:{},checkpoint_interval_secs:{},batch_sink_interval_secs:{}}}", self.buffer_size, self.checkpoint_interval_secs, self.batch_sink_interval_secs)
            }
        }
    }
}
