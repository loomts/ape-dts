#[derive(Clone, Default)]
pub struct ResumerConfig {
    pub tb_positions: String,
    pub finished_tbs: String,
    pub resume_from_log: bool,
    pub resume_log_dir: String,
}
