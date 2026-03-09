use crate::Identifiable;
use dashmap::DashMap;
use flume::{Receiver, Sender};
use lru::LruCache;
use memmap2::{Mmap, MmapMut, MmapOptions};
use speedy::{LittleEndian, Readable, Writable};
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::runtime::Builder;
use tokio::sync::RwLock;
use tracing::{debug, error};

const SEGMENT_SIZE: u64 = 200 * 1024 * 1024;

pub struct SegmentedLogWriter<T>
where
    for<'a> T: Readable<'a, LittleEndian>,
    T: Writable<LittleEndian>,
    T: Identifiable,
{
    _p: PhantomData<T>,
    base_path: PathBuf,
    segment_size: u64,
    current_segment_id: usize,
    mmap: MmapMut,
    // how many entries per segment_id exist
    history: Arc<DashMap<usize, usize>>,
    done: Arc<RwLock<Vec<usize>>>,
    is_cleaned: bool,
    cursor: usize,
    batch: Vec<u8>,
}

#[derive(Clone)]
pub struct SegmentedLogReader<T>
where
    for<'a> T: Readable<'a, LittleEndian>,
    T: Writable<LittleEndian>,
    T: Identifiable,
{
    _p: PhantomData<T>,
    base_path: PathBuf,
    mmap_cache: Arc<RwLock<LruCache<(PathBuf, usize), Mmap>>>,
}

pub struct SegmentedLogCleaner {
    cleaner_rx: Receiver<usize>,
    pub cleaner_tx: Sender<usize>,
    base_path: PathBuf,
    handle: Option<JoinHandle<()>>,
    history: Arc<DashMap<usize, usize>>,
    done: Arc<RwLock<Vec<usize>>>,
}

impl SegmentedLogCleaner {
    pub fn new(
        base_path: PathBuf,
        history: Arc<DashMap<usize, usize>>,
        done: Arc<RwLock<Vec<usize>>>,
    ) -> Self {
        let channel = flume::unbounded();
        Self {
            cleaner_rx: channel.1.clone(),
            cleaner_tx: channel.0.clone(),
            base_path,
            handle: None,
            history,
            done,
        }
    }

    pub async fn clean(&self, segment_id: usize) {
        self.cleaner_tx.send(segment_id).unwrap();
    }

    pub fn start(&mut self) {
        let rx = self.cleaner_rx.clone();
        let base_path = self.base_path.clone();

        let history = self.history.clone();
        let done = self.done.clone();
        let handle = thread::spawn(move || {
            let builder = Builder::new_current_thread().enable_all().build().unwrap();
            builder.block_on(async move {
                loop {
                    if let Ok(segment_id) = rx.recv() {
                        let mut delete = false;
                        if let Some(mut val) = history.get_mut(&segment_id) {
                            *val -= 1;
                            if *val == 0 && done.read().await.contains(&segment_id) {
                                delete = true;
                            }
                        }

                        if delete {
                            let path = base_path.join(format!("segment_{:06}.log", segment_id));
                            history.remove(&segment_id).unwrap();

                            let mut done = done.write().await;

                            if let Some(pos) = done.iter().position(|&x| x == segment_id) {
                                done.remove(pos);
                            }
                            fs::remove_file(path.clone()).await.unwrap();
                            error!("cleaned {}", path.display());
                        }
                    }
                }
            });
            error!("end cleaner");
        });
        self.handle = Some(handle);
    }
}

impl<T: for<'a> speedy::Readable<'a, LittleEndian> + speedy::Writable<LittleEndian> + Identifiable>
    SegmentedLogWriter<T>
{
    pub async fn async_default() -> anyhow::Result<Self> {
        Self::new("wal_segments").await
    }

    pub async fn build_reader(&self) -> anyhow::Result<SegmentedLogReader<T>> {
        SegmentedLogReader::new(self.base_path.to_str().unwrap()).await
    }

    pub async fn new(base_path: &str) -> anyhow::Result<Self> {
        let base = PathBuf::from(base_path);
        if base.exists() {
            fs::remove_dir_all(base.as_path()).await?
        }

        fs::create_dir_all(&base).await?;

        let first_segment_id = 0;
        let mmap = Self::map_segment(&base, first_segment_id, SEGMENT_SIZE).await;

        Ok(Self {
            _p: Default::default(),
            base_path: base,
            segment_size: SEGMENT_SIZE,
            current_segment_id: first_segment_id,
            mmap,
            history: Default::default(),
            done: Arc::new(Default::default()),
            is_cleaned: false,
            cursor: 0,
            batch: vec![],
        })
    }

    pub fn build_cleaner(&mut self) -> SegmentedLogCleaner {
        self.is_cleaned = true;
        let mut cleaner = SegmentedLogCleaner::new(
            self.base_path.clone(),
            self.history.clone(),
            self.done.clone(),
        );
        cleaner.start();
        cleaner
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
    async fn rotate(&mut self) {
        // Ensure data is synced before switching
        self.mmap.flush().unwrap();

        if self.is_cleaned {
            self.history
                .entry(self.current_segment_id)
                .and_modify(|v| *v += 1)
                .or_insert(1);
            self.done.write().await.push(self.current_segment_id);
        }

        self.current_segment_id += 1;
        self.cursor = 0;
        self.mmap =
            Self::map_segment(&self.base_path, self.current_segment_id, self.segment_size).await;

        debug!("Rotated to segment {}", self.current_segment_id);
    }

    /// this expects records to no be empty
    pub async fn log(&mut self, records: &Vec<T>) -> (u64, u64, SegmentedIndex) {
        self.batch.clear();
        let first_id = &records[0].id();
        let len = records.len();

        records.write_to_stream(&mut self.batch).unwrap();

        (*first_id, len as u64, self.write_batch().await)
    }

    #[cfg(test)]
    pub async fn reset(&mut self) -> anyhow::Result<()> {
        if self.base_path.exists() {
            fs::remove_dir_all(self.base_path.clone()).await?
        }
        Ok(())
    }

    pub async fn write_batch(&mut self) -> SegmentedIndex {
        let len = self.batch.len();

        if len > self.segment_size as usize {
            panic!(
                "Batch size {} exceeds maximum segment size {}",
                len, self.segment_size
            );
        }

        if self.cursor + len > self.segment_size as usize {
            self.rotate().await;
        }

        // Copy data to the memory-mapped region
        self.mmap[self.cursor..self.cursor + len].copy_from_slice(&self.batch);
        self.batch.clear();
        let start_pointer = self.cursor;

        // handle current batch
        self.cursor += len;
        self.history
            .entry(self.current_segment_id)
            .and_modify(|v| *v += 1)
            .or_insert(1);
        SegmentedIndex {
            segment_id: self.current_segment_id,
            start_pointer,
            bytes: len,
        }
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

impl<T: for<'a> speedy::Readable<'a, LittleEndian> + speedy::Writable<LittleEndian> + Identifiable>
    SegmentedLogReader<T>
{
    pub async fn new(base_path: &str) -> anyhow::Result<Self> {
        let base = PathBuf::from(base_path);
        fs::create_dir_all(&base).await?;

        Ok(Self {
            _p: Default::default(),
            base_path: base,
            mmap_cache: Arc::new(RwLock::new(LruCache::new(NonZeroUsize::new(10).unwrap()))),
        })
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

    pub async fn read(&self, segment: &SegmentedIndex) -> Vec<u8> {
        let key = (self.base_path.clone(), segment.segment_id);
        let end_pointer = segment.start_pointer + segment.bytes;

        {
            let mut cache = self.mmap_cache.write().await;
            // Note: LruCache::get requires a mutable reference to update "recency"
            if let Some(mmap) = cache.get(&key) {
                return mmap[segment.start_pointer..end_pointer].to_vec();
            }
        }

        // 2. Cache miss: Map the segment
        let mmap = Self::map_segment_read(&self.base_path, segment.segment_id).await;
        let result = mmap[segment.start_pointer..end_pointer].to_vec();

        // 3. Re-acquire WRITE lock to insert
        let mut cache = self.mmap_cache.write().await;
        cache.put(key, mmap);

        result
    }

    pub async fn unlog(&self, batch: &SegmentedIndex) -> Vec<T> {
        let data = self.read(batch).await;
        Vec::<T>::read_from_buffer(&data).expect("Failed to deserialize record")
    }
}

#[derive(Clone, Copy)]
pub struct SegmentedIndex {
    /// which file
    pub segment_id: usize,
    /// where is the first value
    pub start_pointer: usize,
    /// how many bytes?
    pub bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TimedRecord;
    use crate::{InitialMeta, TimedMeta};
    use std::vec;
    use value::Value;

    #[tokio::test]
    async fn test_one() {
        let mut log = SegmentedLogWriter::new(&format!("temp/wals/wal_{}", 0))
            .await
            .unwrap();
        let values = vec![TimedRecord::from((
            Value::int(3),
            TimedMeta::new(0, InitialMeta::new(vec![])),
        ))];
        let (_, _, index) = log.log(&values).await;

        let reader = log.build_reader().await.unwrap();
        let values_retrieved = reader.unlog(&index).await;

        log.reset().await.expect("Failed to cleanup test");

        assert_eq!(values, values_retrieved);
    }

    #[tokio::test]
    async fn test_multiple() {
        let mut log = SegmentedLogWriter::new(&format!("temp/wals/wal_{}", 1))
            .await
            .unwrap();
        let values0 = vec![TimedRecord::from((
            Value::int(0),
            TimedMeta::new(0, InitialMeta::new(vec![])),
        ))];
        let _ = log.log(&values0).await;

        let values1 = vec![TimedRecord::from((
            Value::int(1),
            TimedMeta::new(1, InitialMeta::new(vec![])),
        ))];
        let (_, _, index) = log.log(&values1).await;

        let values2 = vec![TimedRecord::from((
            Value::int(2),
            TimedMeta::new(2, InitialMeta::new(vec![])),
        ))];
        let _ = log.log(&values2).await;

        let reader = log.build_reader().await.unwrap();
        let values_retrieved = reader.unlog(&index).await;

        log.reset().await.expect("Failed to cleanup test");

        assert_eq!(values1, values_retrieved);
    }

    #[tokio::test]
    async fn test_parallel() {
        let mut log = SegmentedLogWriter::new(&format!("temp/wals/wal_{}", 3))
            .await
            .unwrap();
        let values = vec![TimedRecord::from((
            Value::int(3),
            TimedMeta::new(0, InitialMeta::new(vec![])),
        ))];
        let (_, _, index) = log.log(&values).await;

        let reader = log.build_reader().await.unwrap();
        let values_retrieved = reader.unlog(&index).await;

        log.reset().await.expect("Failed to cleanup test");

        assert_eq!(values, values_retrieved);
    }
}
