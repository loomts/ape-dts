pub mod cdc_resumer;
pub mod snapshot_resumer;

const CURRENT_POSITION_LOG_FLAG: &str = "| current_position |";
const TAIL_POSITION_COUNT: usize = 100;
