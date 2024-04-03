use strum::EnumString;

#[derive(EnumString, Clone)]
pub enum RedisStatisticType {
    #[strum(serialize = "big_key")]
    BigKey,
    #[strum(serialize = "hot_key")]
    HotKey,
}
