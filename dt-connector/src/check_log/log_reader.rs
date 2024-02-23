use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Lines},
    path::PathBuf,
};

pub struct LogReader {
    files: Vec<PathBuf>,
    file_index: usize,
    lines: Option<Lines<BufReader<File>>>,
}

impl LogReader {
    pub fn new(dir_path: &str) -> Self {
        let files = Self::list_files(dir_path);
        Self {
            files,
            file_index: 0,
            lines: Option::None,
        }
    }

    pub fn nextval(&mut self) -> Option<String> {
        if self.file_index >= self.files.len() {
            return Option::None;
        }

        if self.lines.is_none() {
            let path = &self.files[self.file_index];
            let file = File::open(path.to_str().unwrap()).unwrap();
            self.lines = Some(BufReader::new(file).lines());
        }

        if let Some(lines) = self.lines.as_mut() {
            if let Some(result) = lines.next() {
                return Some(result.unwrap());
            } else {
                self.lines = None;
                self.file_index += 1;
                return self.nextval();
            }
        }
        None
    }

    fn list_files(dir_path: &str) -> Vec<PathBuf> {
        let mut files = fs::read_dir(dir_path)
            .unwrap()
            .filter_map(|entry| {
                let path = entry.unwrap().path();
                if path.is_file() {
                    Some(path)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
        files
    }
}
