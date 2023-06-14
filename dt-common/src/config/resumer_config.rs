use std::collections::HashMap;

#[derive(Clone)]
pub struct ResumerConfig {
    pub resume_values: HashMap<String, Option<String>>,
}
