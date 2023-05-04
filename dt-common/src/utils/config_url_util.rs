use std::env;

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

    // convert_with_envs: format the database_url with envs, such as:
    // change: mysql://{test_user}:{test_password}@{test_url}
    // to: mysql://test:123456@127.0.0.1:3306
    // when have the envs: test_user=test, test_password=123456, test_url=127.0.0.1:3306
    pub fn convert_with_envs(database_url: String) -> Option<String> {
        if database_url.is_empty() {
            return None;
        }
        let (mut new_url_bytes, mut pos, mut left_pos): (Vec<u8>, i64, i64) = (vec![], 0, -1);

        for ch in database_url.chars() {
            if ch == '{' {
                left_pos = pos;
            } else if ch == '}' && pos > left_pos && left_pos >= 0 {
                let new_env = String::from_utf8(
                    database_url.as_bytes()[(left_pos + 1) as usize..pos as usize].to_vec(),
                )
                .unwrap();
                if env::var(&new_env).is_ok() {
                    let env_val_tmp = env::var(new_env).unwrap();
                    new_url_bytes.extend_from_slice(env_val_tmp.as_bytes());
                }
                left_pos = -1;
            } else if left_pos == -1 {
                new_url_bytes.push(ch as u8);
            }
            pos += 1;
        }

        Some(String::from_utf8(new_url_bytes).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_username_test() {
        assert_eq!(
            ConfigUrlUtil::get_username(String::from(
                "postgres://postgres:123456@127.0.0.1:5431/dt_test"
            ))
            .unwrap(),
            "postgres"
        );

        assert_eq!(
            ConfigUrlUtil::get_username(String::from(
                "mysql://root:123456@127.0.0.1:3306?ssl-mode=disabled"
            ))
            .unwrap(),
            "root"
        );

        // unnormal case
        assert_eq!(
            ConfigUrlUtil::get_username(String::from(
                "mysql:///root:123456@127.0.0.1:3306?ssl-mode=disabled"
            ))
            .unwrap(),
            "/root"
        );
    }

    #[test]
    fn convert_with_envs() {
        env::set_var("test_user", "test");
        env::set_var("test_password", "123456");
        env::set_var("test_url", "127.0.0.1:3306");

        let mut opt: Option<String>;
        opt = ConfigUrlUtil::convert_with_envs(String::from(
            "mysql://{test_user}:{test_password}@{test_url}?ssl-mode=disabled",
        ));
        assert!(
            opt.is_some() && opt.unwrap() == "mysql://test:123456@127.0.0.1:3306?ssl-mode=disabled"
        );

        opt = ConfigUrlUtil::convert_with_envs(String::from(
            "mysql://test:123456@127.0.0.1:3306?ssl-mode=disabled",
        ));
        assert!(
            opt.is_some() && opt.unwrap() == "mysql://test:123456@127.0.0.1:3306?ssl-mode=disabled"
        );

        // unnormal case
        env::set_var("test_wrong", "wrong");
        opt = ConfigUrlUtil::convert_with_envs(String::from(
            "mysql://}test:123456{test_wrong}{@127.0.0.1:3306?ssl-mode=disabled",
        ));
        assert!(opt.is_some() && opt.unwrap() == "mysql://}test:123456wrong");

        env::remove_var("test_user");
        env::remove_var("test_password");
        env::remove_var("test_url");
        env::remove_var("test_wrong");
    }
}
