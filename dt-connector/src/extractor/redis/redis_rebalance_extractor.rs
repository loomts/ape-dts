use std::collections::HashMap;

pub struct RedisRebalanceExtractor {
    pub slot_node_map: HashMap<u16, &'static str>,
}

impl RedisRebalanceExtractor {}
