use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

pub struct FileUtil {}

impl FileUtil {
    pub fn tail(path: &str, n: usize) -> anyhow::Result<Vec<String>> {
        let mut file = File::open(path)?;
        file.seek(SeekFrom::End(0))?;

        let mut cur_char: [u8; 1] = [0];
        let mut read_lines = 0;

        while n > read_lines {
            let i = match file.seek(SeekFrom::Current(-1)) {
                Ok(i) => i,
                Err(_) => break,
            };

            if i == 0 {
                break;
            }

            if file.read(&mut cur_char)? != 1 {
                continue;
            }

            file.seek(SeekFrom::Current(-1))?;
            if cur_char[0] as char == '\n' {
                read_lines += 1;
            }
        }

        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        let lines: Vec<String> = buf.split('\n').map(|i| i.to_string()).collect();
        Ok(lines)
    }
}
