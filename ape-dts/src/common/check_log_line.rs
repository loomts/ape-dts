use crate::meta::row_data::RowData;

pub struct CheckLogLine {
    pub schema: String,
    pub tb: String,
    pub cols: Vec<String>,
    pub col_values: Vec<String>,
}

impl CheckLogLine {
    pub fn to_string(&self) -> String {
        let mut str = format!("{},{}", self.schema, self.tb);
        for i in 0..self.cols.len() {
            str = format!(
                "{},{},{},{}",
                str,
                self.cols[i],
                self.col_values[i].len(),
                self.col_values[i]
            );
        }
        str
    }

    pub fn from_row_data(row_data: &RowData, id_cols: &Vec<String>) -> Self {
        let after = row_data.after.as_ref().unwrap();
        let mut col_values = Vec::new();
        for col in id_cols {
            col_values.push(after.get(col).unwrap().to_string());
        }

        Self {
            schema: row_data.db.clone(),
            tb: row_data.tb.clone(),
            cols: id_cols.clone(),
            col_values,
        }
    }

    pub fn from_string(line: String) -> Self {
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
            let col_value_len = col_value_len.parse::<usize>().unwrap();
            let (col_value, index) = Self::read_token_by_len(&chars, index, col_value_len);

            start_index = index;
            cols.push(col);
            col_values.push(col_value);
        }

        Self {
            schema,
            tb,
            cols,
            col_values,
        }
    }

    fn read_token_by_len(chars: &Vec<char>, start_index: usize, len: usize) -> (String, usize) {
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
