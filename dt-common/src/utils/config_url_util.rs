pub struct ConfigUrlUtil {}

impl ConfigUrlUtil {
    // get_username: try to get the username from a databaseUrl
    // postgres://postgres:123456@127.0.0.1:5431/dt_test
    // mysql://root:123456@127.0.0.1:3306?ssl-mode=disabled
    pub fn get_username(database_url: String) -> Option<String> {
        if database_url.is_empty() {
            return None;
        }
        match database_url.split(':').nth(1) {
            Some(username) => {
                let byte_arr = username.as_bytes();
                return Some(String::from_utf8(byte_arr[2..].to_vec()).unwrap());
            }
            None => None,
        }
    }
}
