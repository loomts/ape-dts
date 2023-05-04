use strum::{AsStaticStr, EnumString};

#[derive(Clone)]
pub struct StructConfig {
    pub conflict_policy: ConflictPolicyEnum,
    pub is_do_check: bool,
}

#[derive(Clone, EnumString, AsStaticStr)]
pub enum ConflictPolicyEnum {
    #[strum(serialize = "ignore")]
    Ignore,
    #[strum(serialize = "interrupt")]
    Interrupt,
}
