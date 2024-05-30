#[derive(Clone, Debug)]
pub struct S3Config {
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub bucket: String,
    pub root_dir: String,
    pub root_url: String,
}
