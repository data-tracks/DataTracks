use crate::TimedRecord;
use memmap2::{Mmap, MmapMut, MmapOptions};
use speedy::{Readable, Writable};
use std::error::Error;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::fs::OpenOptions;
use tracing::debug;

pub struct SegmentedLog {
    base_path: PathBuf,
    segment_size: u64,
    current_segment_id: usize,
    mmap: MmapMut,
    cursor: usize,
    batch: Vec<u8>,
}

const SEGMENT_SIZE: u64 = 10 * 1024 * 1024;

impl SegmentedLog {
    pub async fn async_default() -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::new("wal_segments", SEGMENT_SIZE).await // 10 MB segments
    }

    pub async fn new(
        base_path: &str,
        segment_size: u64,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let base = PathBuf::from(base_path);
        if base.exists() {
            fs::remove_dir_all(base.as_path()).await?
        }

        fs::create_dir_all(&base).await?;

        let first_segment_id = 0;
        let mmap = Self::map_segment(&base, first_segment_id, segment_size).await;

        Ok(Self {
            base_path: base,
            segment_size,
            current_segment_id: first_segment_id,
            mmap,
            cursor: 0,
            batch: vec![],
        })
    }

    async fn map_segment(base: &Path, id: usize, size: u64) -> MmapMut {
        let path = base.join(format!("segment_{:06}.log", id));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await
            .expect("Failed to open segment");


        file.set_len(size).await.expect("Failed to set file size");

        // theoretically other processes could change the file, while we are writing
        unsafe {
            MmapOptions::new()
                .populate()
                .map_mut(&file)
                .expect("Failed to mmap")
        }
    }

    async fn map_segment_read(base: &Path, id: usize) -> Mmap {
        let path = base.join(format!("segment_{:06}.log", id));
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .await
            .expect("Failed to open segment");

        unsafe { MmapOptions::new().map(&file).unwrap() }
    }

    async fn rotate(&mut self) {
        // Ensure data is synced before switching
        self.mmap.flush().unwrap();

        self.current_segment_id += 1;
        self.cursor = 0;
        self.mmap =
            Self::map_segment(&self.base_path, self.current_segment_id, self.segment_size).await;
        debug!("Rotated to segment {}", self.current_segment_id);
    }

    /// this expects records to no be empty
    pub async fn log(&mut self, records: &Vec<TimedRecord>) -> (u64, u64, SegmentedIndex) {
        self.batch.clear();
        let first_id = &records[0].id();
        let len = records.len();

        records.write_to_stream(&mut self.batch).unwrap();

        (*first_id, len as u64, self.write_batch().await)
    }

    pub async fn unlog(&self, batch: SegmentedIndex ) -> Vec<TimedRecord> {
        let data = self.read(batch).await;
        Vec::<TimedRecord>::read_from_buffer(&data).expect("Failed to deserialize record")
    }

    #[cfg(test)]
    pub async fn reset(&mut self) -> anyhow::Result<()> {
        if self.base_path.exists() {
            fs::remove_dir_all(self.base_path.clone()).await?
        }
        Ok(())
    }

    pub async fn write_batch(&mut self) -> SegmentedIndex {
        if self.cursor + self.batch.len() > self.segment_size as usize {
            self.rotate().await;
        }

        // Copy data to the memory-mapped region
        self.mmap[self.cursor..self.cursor + self.batch.len()].copy_from_slice(&self.batch);
        let start_pointer = self.cursor;

        // handle current batch
        let bytes = self.batch.len();
        self.cursor += bytes;
        (self.cursor, self.current_segment_id);
        SegmentedIndex{
            segment_id: self.current_segment_id,
            start_pointer,
            bytes,
        }
    }

    pub async fn read(&self, segment: SegmentedIndex) -> Vec<u8> {
        let mut values: Vec<u8> = vec![];

        let end_pointer = segment.start_pointer + segment.bytes;
        if self.current_segment_id == segment.segment_id {
            values.append(self.mmap[segment.start_pointer..end_pointer].to_vec().as_mut());
        }else {
            let mmap = Self::map_segment_read(&self.base_path, segment.segment_id).await;
            values.append(mmap[segment.start_pointer..end_pointer].to_vec().as_mut());
        }

        values
    }

    pub async fn write(&mut self, data: &[u8]) {
        if self.cursor + data.len() > self.segment_size as usize {
            self.rotate().await;
        }

        // Copy data to the memory-mapped region
        self.mmap[self.cursor..self.cursor + data.len()].copy_from_slice(data);
        self.cursor += data.len();
    }
}

pub struct SegmentedIndex{
    /// which file
    segment_id: usize,
    /// where is the first value
    start_pointer: usize,
    /// how many bytes?
    bytes: usize
}


#[cfg(test)]
mod tests {
    use std::vec;
    use value::Value;
    use crate::{InitialMeta, TimedMeta};
    use super::*;

    #[tokio::test]
    async fn test_one() {
        let mut log = SegmentedLog::new(&format!("wals/wal_{}", 0), SEGMENT_SIZE)
            .await
            .unwrap();
        let values = vec![TimedRecord::from((Value::int(3), TimedMeta::new(0, InitialMeta::new(vec![]))))];
        let (_, _, index) = log.log(&values).await;
        let values_retrieved = log.unlog(index).await;

        log.reset().await.expect("Failed to cleanup test");

        assert_eq!(values, values_retrieved);
    }

    #[tokio::test]
    async fn test_multiple() {
        let mut log = SegmentedLog::new(&format!("wals/wal_{}", 1), SEGMENT_SIZE)
            .await
            .unwrap();
        let values0 = vec![TimedRecord::from((Value::int(0), TimedMeta::new(0, InitialMeta::new(vec![]))))];
        let _ = log.log(&values0).await;

        let values1 = vec![TimedRecord::from((Value::int(1), TimedMeta::new(1, InitialMeta::new(vec![]))))];
        let (_, _, index) = log.log(&values1).await;

        let values2 = vec![TimedRecord::from((Value::int(2), TimedMeta::new(2, InitialMeta::new(vec![]))))];
        let _ = log.log(&values2).await;

        let values_retrieved = log.unlog(index).await;

        log.reset().await.expect("Failed to cleanup test");

        assert_eq!(values1, values_retrieved);
    }

}