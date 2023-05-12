use std::cmp;

use crate::meta::{rdb_tb_meta::RdbTbMeta, row_data::RowData};

use super::log_type::LogType;

pub struct CheckLog {
    pub schema: String,
    pub tb: String,
    pub cols: Vec<String>,
    pub col_values: Vec<Option<String>>,
    pub log_type: LogType,
}

const NULL_VALUE_LEN: i32 = -1;

impl CheckLog {
    pub fn to_string(&self) -> String {
        // example: 3,db1|3,tb1|4,col1|3,abc|4,col2|-1,
        let mut str = format!(
            "{},{}|{},{}",
            self.schema.len(),
            self.schema,
            self.tb.len(),
            self.tb
        );

        for i in 0..self.cols.len() {
            let (value_len, value) = if let Some(value) = &self.col_values[i] {
                (value.len() as i32, value.as_str())
            } else {
                (NULL_VALUE_LEN, "")
            };

            str = format!(
                "{}|{},{}|{},{}",
                str,
                self.cols[i].len(),
                self.cols[i],
                value_len,
                value
            );
        }
        str
    }

    pub fn from_row_data(row_data: &RowData, tb_meta: &RdbTbMeta, log_type: LogType) -> Self {
        let after = row_data.after.as_ref().unwrap();
        let mut col_values = Vec::new();
        for col in tb_meta.id_cols.iter() {
            col_values.push(after.get(col).unwrap().to_option_string());
        }

        Self {
            schema: row_data.schema.clone(),
            tb: row_data.tb.clone(),
            cols: tb_meta.id_cols.clone(),
            col_values,
            log_type,
        }
    }

    pub fn from_str(line: &str, log_type: LogType) -> Self {
        // [schema length],schema|[tb length],tb|[col1 name length],[col1 name]|[col1 value length],[col1 value]|[col2 name length],[col2 name]|[col2 value length],[col2 value]|
        // example 1: 3,db1|3,tb1|4,col1|4,1112|4,col2|5,abcde|
        // example 2: 3,db1|3,tb1|4,col1|-1,|4,col2|-1,|
        let chars: Vec<char> = line.chars().collect();
        let mut tokens = Vec::new();

        let mut start_index = 0;
        while start_index < chars.len() {
            let (token, next_index) = Self::read_token(&chars, start_index, ',');
            start_index = next_index;
            if token.is_none() {
                start_index += 1;
            }
            tokens.push(token);
        }

        let schema = tokens[0].clone().unwrap();
        let tb = tokens[1].clone().unwrap();

        let mut i = 2;
        let mut cols = Vec::new();
        let mut col_values = Vec::new();
        while i < tokens.len() - 1 {
            cols.push(tokens[i].clone().unwrap());
            col_values.push(tokens[i + 1].clone());
            i += 2;
        }

        Self {
            schema,
            tb,
            cols,
            col_values,
            log_type,
        }
    }

    fn read_token(
        chars: &Vec<char>,
        start_index: usize,
        split_char: char,
    ) -> (Option<String>, usize) {
        // example 1: "4,abcd"
        // example 2: "-1,"
        let (token_len, next_index) = Self::read_token_by_end_char(&chars, start_index, split_char);
        let token_len = token_len.parse::<i32>().unwrap();
        if token_len == NULL_VALUE_LEN {
            return (Option::None, next_index);
        }
        let (token, next_index) = Self::read_token_by_len(&chars, next_index, token_len);
        (Some(token), next_index)
    }

    fn read_token_by_len(chars: &Vec<char>, start_index: usize, len: i32) -> (String, usize) {
        let len = cmp::max(len, 0) as usize;
        let slice = &chars[start_index..start_index + len];
        let token: String = slice.into_iter().collect();
        let next_index = start_index + len + 1;
        (token, next_index)
    }

    fn read_token_by_end_char(
        chars: &Vec<char>,
        start_index: usize,
        end_char: char,
    ) -> (String, usize) {
        let mut token = String::new();
        for i in start_index..chars.len() {
            let c = chars[i];
            if c == end_char {
                break;
            } else {
                token.push(c);
            }
        }

        let next_index = start_index + token.len() + 1;
        (token, next_index)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_check_log() {
        // col1, col2 values are none
        let str = "3,db1|3,tb1|4,col1|-1,|4,col2|-1,";
        let log = CheckLog::from_str(str, LogType::Diff);
        assert_eq!(log.schema, "db1");
        assert_eq!(log.tb, "tb1");
        assert_eq!(log.cols, vec!["col1", "col2"]);
        assert_eq!(log.col_values, vec![Option::None, Option::None]);
        assert_eq!(str, log.to_string());

        // col1, col2 values is empty
        let str = "3,db1|3,tb1|4,col1|0,|4,col2|0,";
        let log = CheckLog::from_str(str, LogType::Diff);
        assert_eq!(log.schema, "db1");
        assert_eq!(log.tb, "tb1");
        assert_eq!(log.cols, vec!["col1", "col2"]);
        assert_eq!(
            log.col_values,
            vec![Some(String::new()), Some(String::new())]
        );
        assert_eq!(str, log.to_string());

        let str = "3,db1|3,tb1|4,col1|4,1234|4,col2|5,abcde";
        let log = CheckLog::from_str(str, LogType::Diff);
        assert_eq!(log.schema, "db1");
        assert_eq!(log.tb, "tb1");
        assert_eq!(log.cols, vec!["col1", "col2"]);
        assert_eq!(
            log.col_values,
            vec![Some("1234".to_string()), Some("abcde".to_string())]
        );
        assert_eq!(str, log.to_string());

        let str = "3,db1|3,tb1";
        let log = CheckLog::from_str(str, LogType::Diff);
        assert_eq!(log.schema, "db1");
        assert_eq!(log.tb, "tb1");
        assert_eq!(log.cols.len(), 0);
        assert_eq!(log.col_values.len(), 0);
        assert_eq!(str, log.to_string());
    }
}
