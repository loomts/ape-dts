#[derive(Clone, Default)]
pub struct ResumerConfig {
    pub resume_config_file: String,
    pub resume_from_log: bool,
    pub resume_log_dir: String,
}
