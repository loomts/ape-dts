pub struct ConfigTokenParser {}

impl ConfigTokenParser {
    pub fn parse(config: &str, delimiters: &[char], escape_pairs: &[(char, char)]) -> Vec<String> {
        let chars: Vec<char> = config.chars().collect();
        let mut start_index = 0;
        let mut tokens = Vec::new();

        loop {
            let (token, next_index) =
                Self::read_token(&chars, start_index, delimiters, escape_pairs);
            // trim white spaces
            tokens.push(token.trim().to_string());
            // reach the end of chars
            if next_index >= chars.len() {
                break;
            }
            // skip the token_delimiter
            start_index = next_index + 1;
        }

        tokens
    }

    fn read_token(
        chars: &[char],
        start_index: usize,
        delimiters: &[char],
        escape_pairs: &[(char, char)],
    ) -> (String, usize) {
        // read token surrounded by escapes: `db.2`
        for (escape_left, escape_right) in escape_pairs.iter() {
            if chars[start_index] == *escape_left {
                return Self::read_token_with_escape(
                    chars,
                    start_index,
                    (*escape_left, *escape_right),
                );
            }
        }
        Self::read_token_to_delimiter(chars, start_index, delimiters)
    }

    fn read_token_to_delimiter(
        chars: &[char],
        start_index: usize,
        delimiters: &[char],
    ) -> (String, usize) {
        let mut token = String::new();
        for c in chars.iter().skip(start_index) {
            if delimiters.contains(c) {
                break;
            } else {
                token.push(*c);
            }
        }

        let next_index = start_index + token.len();
        (token, next_index)
    }

    fn read_token_with_escape(
        chars: &[char],
        start_index: usize,
        escape_pair: (char, char),
    ) -> (String, usize) {
        let mut start = false;
        let mut token = String::new();
        for c in chars.iter().skip(start_index) {
            if start && *c == escape_pair.1 {
                token.push(*c);
                break;
            }
            if *c == escape_pair.0 {
                start = true;
            }
            if start {
                token.push(*c);
            }
        }

        let next_index = start_index + token.len();
        (token, next_index)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_parse_mysql_filter_config_tokens() {
        let config = r#"db_1.tb_1,`db.2`.`tb.2`,`db"3`.tb_3,db_4.`tb"4`,db_5.*,`db.6`.*,db_7*.*,`db.8*`.*,*.*,`*`.`*`"#;
        let delimiters = vec!['.', ','];
        let escape_pairs = vec![('`', '`')];

        let tokens = ConfigTokenParser::parse(config, &delimiters, &escape_pairs);
        assert_eq!(tokens.len(), 20);
        assert_eq!(tokens[0], "db_1");
        assert_eq!(tokens[1], "tb_1");
        assert_eq!(tokens[2], "`db.2`");
        assert_eq!(tokens[3], "`tb.2`");
        assert_eq!(tokens[4], r#"`db"3`"#);
        assert_eq!(tokens[5], "tb_3");
        assert_eq!(tokens[6], "db_4");
        assert_eq!(tokens[7], r#"`tb"4`"#);
        assert_eq!(tokens[8], "db_5");
        assert_eq!(tokens[9], "*");
        assert_eq!(tokens[10], "`db.6`");
        assert_eq!(tokens[11], "*");
        assert_eq!(tokens[12], "db_7*");
        assert_eq!(tokens[13], "*");
        assert_eq!(tokens[14], "`db.8*`");
        assert_eq!(tokens[15], "*");
        assert_eq!(tokens[16], "*");
        assert_eq!(tokens[17], "*");
        assert_eq!(tokens[18], "`*`");
        assert_eq!(tokens[19], "`*`");
    }

    #[test]
    fn test_parse_mysql_router_config_tokens() {
        let config = r#"db_1.tb_1:`db.2`.`tb.2`,`db"3`.tb_3:db_4.`tb"4`"#;
        let delimiters = vec!['.', ',', ':'];
        let escape_pairs = vec![('`', '`')];

        let tokens = ConfigTokenParser::parse(config, &delimiters, &escape_pairs);
        assert_eq!(tokens.len(), 8);
        assert_eq!(tokens[0], "db_1");
        assert_eq!(tokens[1], "tb_1");
        assert_eq!(tokens[2], "`db.2`");
        assert_eq!(tokens[3], "`tb.2`");
        assert_eq!(tokens[4], r#"`db"3`"#);
        assert_eq!(tokens[5], "tb_3");
        assert_eq!(tokens[6], "db_4");
        assert_eq!(tokens[7], r#"`tb"4`"#);
    }

    #[test]
    fn test_parse_pg_filter_config_tokens() {
        let config = r#"db_1.tb_1,"db.2"."tb.2","db`3".tb_3,db_4."tb`4",db_5.*,"db.6".*,db_7*.*,"db.8*".*,*.*,"*"."*""#;
        let delimiters = vec!['.', ','];
        let escape_pairs = vec![('"', '"')];

        let tokens = ConfigTokenParser::parse(config, &delimiters, &escape_pairs);
        assert_eq!(tokens.len(), 20);
        assert_eq!(tokens[0], "db_1");
        assert_eq!(tokens[1], "tb_1");
        assert_eq!(tokens[2], r#""db.2""#);
        assert_eq!(tokens[3], r#""tb.2""#);
        assert_eq!(tokens[4], r#""db`3""#);
        assert_eq!(tokens[5], "tb_3");
        assert_eq!(tokens[6], "db_4");
        assert_eq!(tokens[7], r#""tb`4""#);
        assert_eq!(tokens[8], "db_5");
        assert_eq!(tokens[9], "*");
        assert_eq!(tokens[10], r#""db.6""#);
        assert_eq!(tokens[11], "*");
        assert_eq!(tokens[12], "db_7*");
        assert_eq!(tokens[13], "*");
        assert_eq!(tokens[14], r#""db.8*""#);
        assert_eq!(tokens[15], "*");
        assert_eq!(tokens[16], "*");
        assert_eq!(tokens[17], "*");
        assert_eq!(tokens[18], r#""*""#);
        assert_eq!(tokens[19], r#""*""#);
    }

    #[test]
    fn test_parse_pg_router_config_tokens() {
        let config = r#"db_1.tb_1:"db.2"."tb.2","db`3".tb_3:db_4."tb`4""#;
        let delimiters = vec!['.', ',', ':'];
        let escape_pairs = vec![('"', '"')];

        let tokens = ConfigTokenParser::parse(config, &delimiters, &escape_pairs);
        assert_eq!(tokens.len(), 8);
        assert_eq!(tokens[0], "db_1");
        assert_eq!(tokens[1], "tb_1");
        assert_eq!(tokens[2], r#""db.2""#);
        assert_eq!(tokens[3], r#""tb.2""#);
        assert_eq!(tokens[4], r#""db`3""#);
        assert_eq!(tokens[5], "tb_3");
        assert_eq!(tokens[6], "db_4");
        assert_eq!(tokens[7], r#""tb`4""#);
    }
}
