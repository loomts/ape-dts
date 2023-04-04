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
        let mut str = format!("{},{}", self.schema, self.tb);
        for i in 0..self.cols.len() {
            if let Some(value) = &self.col_values[i] {
                str = format!("{},{},{},{}", str, self.cols[i], value.len(), value);
            } else {
                str = format!("{},{},{},{}", str, self.cols[i], NULL_VALUE_LEN, "");
            }
        }
        str
    }

    pub fn from_row_data(row_data: &RowData, tb_meta: &RdbTbMeta, log_type: LogType) -> Self {
        let after = row_data.after.as_ref().unwrap();
        let mut col_values = Vec::new();
        for col in tb_meta.id_cols.iter() {
            col_values.push(after.get(col).unwrap().to_string());
        }

        Self {
            schema: row_data.db.clone(),
            tb: row_data.tb.clone(),
            cols: tb_meta.id_cols.clone(),
            col_values,
            log_type,
        }
    }

    pub fn from_str(line: &str, log_type: LogType) -> Self {
        // schema,tb,[col1 name],[col1 value length],[col1 value],[col2 name],[col2 value length],[col2 value]
        // example: db1,tb1,col1,4,1112,col2,5,abcde
        let chars: Vec<char> = line.chars().collect();
        let end_char = ',';

        let (schema, index) = Self::read_token(&chars, 0, end_char);
        let (tb, index) = Self::read_token(&chars, index, end_char);
        let mut cols = Vec::new();
        let mut col_values = Vec::new();

        let mut start_index = index;
        while start_index < chars.len() {
            let (col, index) = Self::read_token(&chars, start_index, end_char);
            let (col_value_len, index) = Self::read_token(&chars, index, end_char);
            let col_value_len = col_value_len.parse::<i32>().unwrap();
            let (col_value, index) = Self::read_token_by_len(&chars, index, col_value_len);

            start_index = index;
            cols.push(col);
            if col_value_len == NULL_VALUE_LEN {
                col_values.push(Option::None);
            } else {
                col_values.push(Some(col_value));
            }
        }

        Self {
            schema,
            tb,
            cols,
            col_values,
            log_type,
        }
    }

    fn read_token_by_len(chars: &Vec<char>, start_index: usize, len: i32) -> (String, usize) {
        let len = cmp::max(len, 0) as usize;
        let slice = &chars[start_index..start_index + len];
        let token: String = slice.into_iter().collect();
        let next_index = start_index + len + 1;
        (token, next_index)
    }

    fn read_token(chars: &Vec<char>, start_index: usize, end_char: char) -> (String, usize) {
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
        let str = "db1,tb1,col1,-1,,col2,-1,";
        let log = CheckLog::from_str(str, LogType::Diff);
        assert_eq!(log.schema, "db1");
        assert_eq!(log.tb, "tb1");
        assert_eq!(log.cols, vec!["col1", "col2"]);
        assert_eq!(log.col_values, vec![Option::None, Option::None]);
        assert_eq!(str, log.to_string());

        // col1, col2 values is empty
        let str = "db1,tb1,col1,0,,col2,0,";
        let log = CheckLog::from_str(str, LogType::Diff);
        assert_eq!(log.schema, "db1");
        assert_eq!(log.tb, "tb1");
        assert_eq!(log.cols, vec!["col1", "col2"]);
        assert_eq!(
            log.col_values,
            vec![Some("".to_string()), Some("".to_string())]
        );
        assert_eq!(str, log.to_string());

        let str = "db1,tb1,col1,4,1234,col2,5,abcde";
        let log = CheckLog::from_str(str, LogType::Diff);
        assert_eq!(log.schema, "db1");
        assert_eq!(log.tb, "tb1");
        assert_eq!(log.cols, vec!["col1", "col2"]);
        assert_eq!(
            log.col_values,
            vec![Some("1234".to_string()), Some("abcde".to_string())]
        );
        assert_eq!(str, log.to_string());

        let str = "db1,tb1";
        let log = CheckLog::from_str(str, LogType::Diff);
        assert_eq!(log.schema, "db1");
        assert_eq!(log.tb, "tb1");
        assert_eq!(log.cols.len(), 0);
        assert_eq!(log.col_values.len(), 0);
        assert_eq!(str, log.to_string());

        let str = "db1";
        let log = CheckLog::from_str(str, LogType::Diff);
        assert_eq!(log.schema, "db1");
        assert_eq!(log.tb, "");
        assert_eq!(log.cols.len(), 0);
        assert_eq!(log.col_values.len(), 0);

        let str = "";
        let log = CheckLog::from_str(str, LogType::Diff);
        assert_eq!(log.schema, "");
        assert_eq!(log.tb, "");
        assert_eq!(log.cols.len(), 0);
        assert_eq!(log.col_values.len(), 0);
    }
}
