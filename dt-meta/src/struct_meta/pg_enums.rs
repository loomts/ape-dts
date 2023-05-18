#[allow(dead_code)]
pub enum ConstraintTypeEnum {
    // p -> 112;u -> 117;f -> 102;c -> 99
    Primary,
    Unique,
    Foregin,
    Check,
}

impl ConstraintTypeEnum {
    pub fn to_charval(&self) -> Option<String> {
        let result: String;
        match self {
            ConstraintTypeEnum::Primary => result = String::from("112"),
            ConstraintTypeEnum::Unique => result = String::from("117"),
            ConstraintTypeEnum::Foregin => result = String::from("102"),
            ConstraintTypeEnum::Check => result = String::from("99"),
        }
        Some(result)
    }
}
