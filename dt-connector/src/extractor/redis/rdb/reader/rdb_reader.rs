use crate::extractor::redis::StreamReader;

pub struct RdbReader<'a> {
    pub conn: &'a mut Box<&'a mut (dyn StreamReader + Send + 'a)>,
    pub rdb_length: usize,
    pub position: usize,
    pub copy_raw: bool,
    pub raw_bytes: Vec<u8>,
}

impl RdbReader<'_> {
    pub fn drain_raw_bytes(&mut self) -> Vec<u8> {
        self.raw_bytes.drain(..).collect()
    }
}

impl StreamReader for RdbReader<'_> {
    fn read_bytes(&mut self, length: usize) -> anyhow::Result<Vec<u8>> {
        let buf = self.conn.read_bytes(length).unwrap();
        self.position += length;
        if self.copy_raw {
            self.raw_bytes.extend_from_slice(&buf);
        }
        Ok(buf)
    }
}
