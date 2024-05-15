use url::Url;

use crate::log_info;

pub struct UrlUtil {}

impl UrlUtil {
    #[inline(always)]
    pub fn parse(url: &str) -> anyhow::Result<Url> {
        log_info!("parsing url: {}", url);
        let url_info = Url::parse(url)?;
        log_info!("parsed url_info: {}", url_info);
        Ok(url_info)
    }
}
