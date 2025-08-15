use crate::channel::{Rx, Tx, new_channel};
use crate::command::Command;
use crate::command::Command::Stop;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tracing::{debug, info};

const ASYNC_WORKERS: usize = 20;

/*type BoxedFutureFactory = Box<
    dyn FnOnce(WorkerMeta) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> + Send + 'static,
>;*/

#[derive(Clone)]
pub struct WorkerArgs {
    id: usize,
    name: String,
    depends_on: Vec<usize>,
}

#[derive(Clone)]
pub struct WorkerMeta {
    id: usize,
    pub ins: (Tx<Command>, Rx<Command>),
    pub output_channel: Arc<Tx<Command>>,
    name: String,
    depends_on: Vec<usize>,
}

impl WorkerMeta {
    pub fn should_stop(&self) -> bool {
        matches!(self.ins.1.try_recv(), Ok(Stop(_)))
    }
}

impl From<(WorkerArgs, Arc<Tx<Command>>)> for WorkerMeta {
    fn from(args: (WorkerArgs, Arc<Tx<Command>>)) -> Self {
        let ins = new_channel(format!("{}_in", args.0.name), false);
        Self {
            id: args.0.id,
            ins,
            output_channel: args.1,
            name: args.0.name,
            depends_on: args.0.depends_on,
        }
    }
}

enum Worker {
    Sync(SyncWorker),
    Async(AsyncWorker),
}

/// Represents a single worker thread within the pool.
struct SyncWorker {
    handle: Option<thread::JoinHandle<Result<(), String>>>,
    meta: WorkerMeta,
}

impl SyncWorker {
    /// Creates a new worker thread.
    ///
    /// - If it receives a `Sync` job, it executes the closure directly on its thread.
    /// - If it receives an `Async` job, it spawns the `Future` onto the provided Tokio runtime handle.
    fn new(
        task: Box<dyn FnOnce(WorkerMeta) -> Result<(), String> + Send + 'static>,
        args: WorkerArgs,
        output_channel: Arc<Tx<Command>>,
        finished_ids_handle: Arc<Mutex<Vec<usize>>>,
    ) -> Self {
        let id = args.id;

        let meta = WorkerMeta::from((args, output_channel));
        let meta_clone = meta.clone();

        let thread = thread::spawn(move || {
            debug!("[Worker {}] Executing sync task.", id);
            task(meta_clone)?; // Execute the synchronous closure directly on this worker thread.
            debug!("[Worker {}] Finished sync task.", id);
            finished_ids_handle.lock().unwrap().push(id);
            Ok(())
        });

        Self {
            meta,
            handle: Some(thread),
        }
    }
}

struct AsyncWorker {
    meta: WorkerMeta,
    handle: Option<JoinHandle<Result<(), String>>>,
}

impl AsyncWorker {
    fn new<F, Fut>(
        runtime_handle: Handle,
        future_task: F,
        output_channel: Arc<Tx<Command>>,
        args: WorkerArgs,
        finished_ids_handle: Arc<Mutex<Vec<usize>>>,
    ) -> Self
    where
        F: FnOnce(WorkerMeta) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        debug!(
            "[Worker {}] Spawning async task onto Tokio runtime.",
            args.id
        );
        // Spawn the future onto the shared Tokio runtime using its handle.
        // This operation is non-blocking to the current worker thread;
        // the Tokio runtime's internal threads will execute the future.
        let meta = WorkerMeta::from((args, output_channel));
        let meta_clone = meta.clone();
        let id = meta.id;

        let handle = runtime_handle.spawn(async move {
            future_task(meta_clone).await?;
            debug!("[Worker {}] Finished sync task.", id);
            finished_ids_handle.lock().unwrap().push(meta.id);
            Ok(())
        });

        Self {
            meta,
            handle: Some(handle),
        }
    }
}

pub struct PoolMeta {
    async_workers: usize,
    sync_workers: usize,
}

/// The thread-safe `HybridThreadPool` handle.
/// This handle gives access to the underlying pool which manages a set of standard threads for dispatching/sync tasks
/// and an embedded Tokio runtime for executing async tasks.
#[derive(Clone, Default)]
pub struct HybridThreadPool {
    state: Arc<PoolState>,
}

impl HybridThreadPool {
    pub fn new() -> Self {
        HybridThreadPool {
            state: Arc::new(PoolState::new()),
        }
    }

    pub(crate) fn meta(&self) -> PoolMeta {
        self.state.meta()
    }

    pub fn control_sender(&self) -> Arc<Tx<Command>> {
        self.state.control_sender()
    }

    pub fn control_receiver(&self) -> Arc<Rx<Command>> {
        self.state.control_receiver()
    }

    pub fn send_control(&self, id: &usize, command: Command) -> Result<(), String> {
        self.state.send_control(id, command)
    }

    pub fn stop(&self, id: &usize) -> Result<(), String> {
        self.state.stop(id)
    }

    pub fn join(&self, id: &usize) {
        self.state.join(id).unwrap()
    }

    pub fn execute_async<F, S: AsRef<str>>(&self, name: S, f: F) -> Result<usize, String>
    where
        F: FnOnce(WorkerMeta) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>>
            + Send
            + 'static,
    {
        Ok(self.state.execute_async(name, f, vec![]))
    }

    pub fn execute_sync<F, S: AsRef<str>>(&self, name: S, f: F) -> Result<usize, String>
    where
        F: FnOnce(WorkerMeta) -> Result<(), String> + Send + 'static,
    {
        Ok(self.state.execute_sync(name, f, vec![]))
    }
}

/// The main thread-safe `PoolState` housing all the logic.
/// This pool manages a set of standard threads for dispatching/sync tasks
/// and an embedded Tokio runtime for executing async tasks.
struct PoolState {
    id_counter: AtomicUsize,
    /// A vector holding all the `Worker` instances (and their join handles).
    sync_workers: Arc<Mutex<HashMap<usize, SyncWorker>>>,
    async_workers: Arc<Mutex<HashMap<usize, AsyncWorker>>>,
    /// The Tokio runtime instance that will execute all asynchronous tasks.
    runtime: tokio::runtime::Runtime,
    roots: Arc<Mutex<Vec<usize>>>,
    pub control: (Arc<Tx<Command>>, Arc<Rx<Command>>),
    finished_ids: Arc<Mutex<Vec<usize>>>,
    cleanup: thread::JoinHandle<()>,
    cleanup_tx: Tx<Command>,
}

impl Default for PoolState {
    fn default() -> Self {
        Self::new()
    }
}

impl PoolState {
    /// Creates a new `HybridThreadPool`.
    fn new() -> Self {
        // Build a multithreaded Tokio runtime. This runtime is responsible
        // for scheduling and executing all `async` tasks.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(ASYNC_WORKERS) // Configure number of threads for Tokio's scheduler
            .thread_name("hybrid-async-runtime-worker") // Custom name for Tokio's internal threads
            .enable_all() // Enable all features (timers, I/O, etc.) for the runtime
            .build()
            .expect("Failed to create Tokio runtime");

        let finished_ids = Arc::new(Mutex::new(vec![]));
        let sync_workers = Arc::new(Mutex::new(HashMap::new()));
        let async_workers = Arc::new(Mutex::new(HashMap::new()));

        let (cleanup_tx, rx) = new_channel("Cleanup Pool", false);
        let finished_clone = finished_ids.clone();
        let async_workers_clone = async_workers.clone();
        let sync_workers_clone = sync_workers.clone();

        let cleanup = thread::spawn(move || {
            loop {
                if let Ok(Stop(_)) = rx.try_recv() {
                    return;
                }
                Self::preform_cleanup(&finished_clone, &async_workers_clone, &sync_workers_clone);
                thread::sleep(Duration::from_millis(100));
            }
        });

        let (c_tx, c_rx) = new_channel("Control Pool", false);

        PoolState {
            id_counter: Default::default(),
            sync_workers,
            async_workers,
            runtime, // The Tokio runtime is moved into the pool structure.
            roots: Arc::new(Mutex::new(Vec::new())),
            control: (Arc::new(c_tx), Arc::new(c_rx)),
            finished_ids,
            cleanup,
            cleanup_tx,
        }
    }

    fn control_sender(&self) -> Arc<Tx<Command>> {
        self.control.0.clone()
    }

    fn control_receiver(&self) -> Arc<Rx<Command>> {
        self.control.1.clone()
    }

    fn stop(&self, id: &usize) -> Result<(), String> {
        if let Some(sync_worker) = self.sync_workers.lock().unwrap().get(id) {
            sync_worker.meta.ins.0.send(Stop(sync_worker.meta.id))
        } else if let Some(async_worker) = self.async_workers.lock().unwrap().get(id) {
            async_worker.meta.ins.0.send(Stop(async_worker.meta.id))
        } else {
            Ok(())
        }
    }

    fn join(&self, id: &usize) -> Result<(), String> {
        if let Some(w) = self.sync_workers.lock().unwrap().get_mut(id) {
            if let Some(handle) = w.handle.take() {
                handle.join().map_err(|_| "Error joining thread")??;
            }
        } else if let Some(w) = self.async_workers.lock().unwrap().get_mut(id)
            && let Some(handle) = w.handle.take() {
            handle.abort();
        }
        Ok(())
    }

    fn meta(&self) -> PoolMeta {
        let async_workers = self.async_workers.lock().unwrap().len();
        let sync_workers = self.sync_workers.lock().unwrap().len();
        PoolMeta {
            async_workers,
            sync_workers,
        }
    }

    fn send_control(&self, num: &usize, command: Command) -> Result<(), String> {
        if let Some(w) = self.sync_workers.lock().unwrap().get_mut(num) {
            w.meta.ins.0.send(command)?;
        } else if let Some(w) = self.async_workers.lock().unwrap().get_mut(num) {
            w.meta.ins.0.send(command)?;
        }
        Ok(())
    }

    /// Submits a synchronous task to the pool.
    /// This task will be picked up by one of the `num_sync_workers` threads
    /// and executed directly on that thread.
    fn execute_sync<F, S: AsRef<str>>(&self, name: S, f: F, depends_on: Vec<usize>) -> usize
    where
        F: FnOnce(WorkerMeta) -> Result<(), String> + Send + 'static,
    {
        let depends_on_clone = depends_on.clone();

        let meta = WorkerArgs {
            id: self.id_counter.fetch_add(1, Ordering::Relaxed),
            name: name.as_ref().to_string(),
            depends_on,
        };
        let id = meta.id;

        self.sync_workers.lock().unwrap().insert(
            meta.id,
            SyncWorker::new(
                Box::new(f),
                meta,
                self.control_sender(),
                self.finished_ids.clone(),
            ),
        );
        if depends_on_clone.is_empty() {
            self.roots.lock().unwrap().push(id);
        };
        id
    }

    /// Submits an asynchronous task to the pool.
    /// This task will be dispatched by a `sync` worker and then spawned onto the
    /// internal Tokio runtime.
    fn execute_async<F, Fut, S: AsRef<str>>(&self, name: S, f: F, depends_on: Vec<usize>) -> usize
    where
        F: FnOnce(WorkerMeta) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        let depends_on_clone = depends_on.clone();

        // When sending an async task, we now use Box::pin to create a Pinned Box.
        let args = WorkerArgs {
            id: self.id_counter.fetch_add(1, Ordering::Relaxed),
            name: name.as_ref().to_string(),
            depends_on,
        };
        let handle = self.runtime.handle().clone();
        let id = args.id;

        self.async_workers.lock().unwrap().insert(
            args.id,
            AsyncWorker::new(
                handle,
                Box::new(f),
                self.control_sender(),
                args,
                self.finished_ids.clone(),
            ),
        );
        if depends_on_clone.is_empty() {
            self.roots.lock().unwrap().push(id);
        }
        id
    }

    fn preform_cleanup(
        finished_ids: &Arc<Mutex<Vec<usize>>>,
        async_workers: &Arc<Mutex<HashMap<usize, AsyncWorker>>>,
        sync_workers: &Arc<Mutex<HashMap<usize, SyncWorker>>>,
    ) {
        // Step 1: Drain all currently signaled completed task IDs into a HashSet for fast lookup.
        let mut completed_ids_guard = finished_ids.lock().unwrap();
        let completed_ids: HashSet<usize> = completed_ids_guard.drain(..).collect();
        drop(completed_ids_guard); // Release the lock on `completed_sync_task_ids` immediately.

        if completed_ids.is_empty() {
            // No new tasks have completed since the last cleanup, so nothing to do.
            return;
        }

        debug!(
            "[Cleanup Thread] Processing {} newly completed IDs.",
            completed_ids.len()
        );

        Self::handle_sync_workers(sync_workers, &completed_ids);
        Self::handle_async_workers(async_workers, &completed_ids);
    }

    fn handle_async_workers(
        async_workers: &Arc<Mutex<HashMap<usize, AsyncWorker>>>,
        completed_ids: &HashSet<usize>,
    ) {
        // Step 2: Filter the main list of active `JoinHandle`s.
        let mut handles_guard = async_workers.lock().unwrap();
        let initial_len = handles_guard.len();
        // Create a new vector to hold only the handles of tasks that are still active.
        let mut new_handles_vec = HashMap::new();

        // Iterate through all tracked handles, draining them from the original vector.
        for (id, mut w) in handles_guard.drain() {
            if completed_ids.contains(&id) {
                if let Some(handle) = w.handle.take() {
                    if handle.is_finished() {
                        // nothing to do
                    } else {
                        handle.abort();
                    }
                }
            } else {
                // This task has not yet signaled completion, so it's either still running
                // or hasn't had a chance to signal. We keep its handle for future cleanup cycles.
                new_handles_vec.insert(id, w);
            }
        }
        // Replace the old vector content with the new, filtered list of active handles.
        *handles_guard = new_handles_vec;

        let final_len = handles_guard.len();
        if initial_len != final_len {
            println!(
                "[Cleanup Thread] Removed {} completed handles. Remaining active: {}.",
                initial_len - final_len,
                final_len
            );
        }
    }

    fn handle_sync_workers(
        sync_workers: &Arc<Mutex<HashMap<usize, SyncWorker>>>,
        completed_ids: &HashSet<usize>,
    ) {
        // Step 2: Filter the main list of active `JoinHandle`s.
        let mut handles_guard = sync_workers.lock().unwrap();
        let initial_len = handles_guard.len();
        // Create a new vector to hold only the handles of tasks that are still active.
        let mut new_handles_vec = HashMap::new();

        // Iterate through all tracked handles, draining them from the original vector.
        for (id, mut w) in handles_guard.drain() {
            if completed_ids.contains(&id) {
                match w.handle.take() {
                    None => {}
                    Some(h) => match h.join() {
                        Ok(_) => debug!("[Cleanup Thread] Sync task {id} joined and cleaned up."),
                        Err(e) => {
                            panic!(
                                "[Cleanup Thread Error] Sync task {id} panicked during cleanup: {e:?}"
                            )
                        }
                    },
                }
            } else {
                // This task has not yet signaled completion, so it's either still running
                // or hasn't had a chance to signal. We keep its handle for future cleanup cycles.
                new_handles_vec.insert(id, w);
            }
        }
        // Replace the old vector content with the new, filtered list of active handles.
        *handles_guard = new_handles_vec;

        let final_len = handles_guard.len();
        if initial_len != final_len {
            println!(
                "[Cleanup Thread] Removed {} completed handles. Remaining active: {}.",
                initial_len - final_len,
                final_len
            );
        }
    }
}

/// Implements the `Drop` trait for `HybridThreadPool` to ensure graceful shutdown.
/// When the `HybridThreadPool` goes out of scope, this code will be executed.
impl Drop for PoolState {
    fn drop(&mut self) {
        info!("[Pool] Shutting down thread pool.");

        // Send a `Terminate` message to each worker.
        // It's crucial to send all termination messages *before* starting to `join()` threads.
        // If we joined threads one by one and the sender was dropped early, other workers
        // might deadlock waiting for a message that will never arrive.
        for w in self.sync_workers.lock().unwrap().values() {
            match w.meta.ins.0.send(Stop(0)) {
                Ok(_) => {}
                Err(err) => println!("Error on drop: {}", err),
            };
        }

        for (_, mut w) in self.sync_workers.lock().unwrap().drain() {
            if let Some(thread) = w.handle.take() {
                match thread.join().unwrap() {
                    Ok(_) => {}
                    Err(err) => println!("Error on drop: {}", err),
                }; // Blocks until the worker thread has finished.
            }
        }

        for w in self.async_workers.lock().unwrap().values() {
            match w.meta.ins.0.send(Stop(0)) {
                Ok(_) => {}
                Err(err) => println!("Error on drop: {}", err),
            };
        }

        for (_, mut w) in self.async_workers.lock().unwrap().drain() {
            if let Some(thread) = w.handle.take() {
                drop(thread) // Blocks until the worker thread has finished.
            }
        }

        // The Tokio runtime `self.runtime` is implicitly shut down when it is dropped
        // at the end of this `drop` implementation.
        info!("[Pool] All workers terminated. Tokio runtime shutting down.");
    }
}

#[cfg(test)]
mod tests {
    use crate::pool::HybridThreadPool;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_sync() {
        let pool = HybridThreadPool::new();
        pool.execute_sync("main", |_args| {
            println!("Hello world!");
            Ok(())
        }).unwrap();

        sleep(Duration::from_secs(1));
        assert_eq!(pool.meta().sync_workers, 0);

        drop(pool);
    }

    #[test]
    fn test_async() {
        let pool = HybridThreadPool::new();
        pool.execute_async("main", |_arg| {
            Box::pin(async {
                println!("Hello world!");
                Ok(())
            })
        }).unwrap();

        sleep(Duration::from_secs(1));
        assert_eq!(pool.meta().async_workers, 0);

        drop(pool);
    }

    #[test]
    fn test_sync_count() {
        let pool = HybridThreadPool::new();
        let lock = Arc::new(AtomicBool::new(false));

        let amount = 10;

        for _ in 0..amount {
            let lock_clone = Arc::clone(&lock);
            pool.execute_sync("t", move |_arg| {
                while !lock_clone.load(Ordering::Relaxed) {
                    sleep(Duration::from_millis(100));
                }
                Ok(())
            }).unwrap();
        }

        sleep(Duration::from_secs(1));
        assert_eq!(pool.meta().sync_workers, amount);
        lock.store(true, Ordering::Relaxed);
        sleep(Duration::from_secs(2));
        assert_eq!(pool.meta().sync_workers, 0);

        drop(pool);
    }

    #[test]
    fn test_async_count() {
        let pool = HybridThreadPool::new();
        let lock = Arc::new(AtomicBool::new(false));

        let amount = 10;

        for _ in 0..amount {
            let lock_clone = Arc::clone(&lock);
            pool.execute_async("t", move |_arg| {
                Box::pin(async move {
                    while !lock_clone.load(Ordering::Relaxed) {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Ok(())
                })
            }).unwrap();
        }

        sleep(Duration::from_secs(1));
        assert_eq!(pool.meta().async_workers, amount);
        lock.store(true, Ordering::Relaxed);
        sleep(Duration::from_secs(1));
        assert_eq!(pool.meta().async_workers, 0);

        drop(pool);
    }

    #[test]
    fn test_sync_sender() {
        let _pool = HybridThreadPool::new();
    }
}
