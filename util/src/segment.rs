use crate::TimedRecord;
use memmap2::{MmapMut, MmapOptions};
use speedy::Writable;
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

impl SegmentedLog {
    pub async fn async_default() -> Result<Self, Box<dyn Error + Send + Sync>> {
        Self::new("wal_segments", 10 * 1024 * 1024).await // 10 MB segments
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

    async fn rotate(&mut self) {
        // Ensure data is synced before switching
        self.mmap.flush().unwrap();

        self.current_segment_id += 1;
        self.cursor = 0;
        self.mmap =
            Self::map_segment(&self.base_path, self.current_segment_id, self.segment_size).await;
        debug!("Rotated to segment {}", self.current_segment_id);
    }

    pub async fn log(&mut self, records: &Vec<TimedRecord>) {
        self.batch.clear();
        for record in records {
            record.write_to_stream(&mut self.batch).unwrap();
            self.batch.push(b'\n');
        }

        self.write_batch().await;
    }

    pub async fn write_batch(&mut self) {
        if self.cursor + self.batch.len() > self.segment_size as usize {
            self.rotate().await;
        }

        // Copy data to the memory-mapped region
        self.mmap[self.cursor..self.cursor + self.batch.len()].copy_from_slice(&self.batch);
        self.cursor += self.batch.len();
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
