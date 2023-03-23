use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Lines},
    path::PathBuf,
    time::SystemTime,
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

    pub fn next(&mut self) -> Option<String> {
        if self.file_index >= self.files.len() {
            return Option::None;
        }

        if self.lines.is_none() {
            let file = File::open(&self.files[self.file_index]).unwrap();
            self.lines = Some(BufReader::new(file).lines());
        }

        if let Some(lines) = self.lines.as_mut() {
            if let Some(result) = lines.next() {
                return Some(result.unwrap());
            } else {
                self.lines = None;
                self.file_index += 1;
                return self.next();
            }
        }
        None
    }

    fn list_files(dir_path: &str) -> Vec<PathBuf> {
        let mut files = fs::read_dir(dir_path)
            .unwrap()
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()
            .unwrap();

        files.sort_by_key(|f| {
            fs::metadata(f)
                .unwrap()
                .created()
                .unwrap_or(SystemTime::UNIX_EPOCH)
        });
        files
    }
}
