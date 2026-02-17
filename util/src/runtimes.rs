use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use tokio::runtime::Runtime;

pub struct Runtimes {
    state: Arc<RwLock<RuntimeStates>>
}


impl Clone for Runtimes {
    fn clone(&self) -> Self {
        Self{
            state: self.state.clone()
        }
    }
}

impl Default for Runtimes {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtimes {
    pub fn new() -> Self {
        Self{
            state: Arc::new(RwLock::new(RuntimeStates::new()))
        }
    }

    pub fn add_runtime(&self, runtime: Runtime) -> u64 {
        let mut state = self.state.write().unwrap();
        let id = state.next_id();
        state.runtimes.insert(id, runtime);
        id
    }


    pub fn attach_runtime(&self, id: &u64, action: impl Future<Output=()> + Send + Sync + 'static) {
        let state = self.state.write().unwrap();
        state.runtimes.get(id).unwrap().spawn(action);
    }

    pub fn add_handle(&self, handle: JoinHandle<()>) -> u64 {
        let mut state = self.state.write().unwrap();
        let id = state.next_id();
        state.handles.insert(id, handle);
        id
    }
}


#[derive(Debug)]
pub struct RuntimeStates {
    runtimes: HashMap<u64, Runtime>,
    handles: HashMap<u64, JoinHandle<()>>,
    id_builder: u64
}

impl Default for RuntimeStates {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeStates {
    pub fn new() -> Self {
        Self{
            runtimes: HashMap::new(),
            handles: HashMap::new(),
            id_builder: 0,
        }
    }

    pub fn next_id(&mut self) -> u64 {
        self.id_builder += 1;
        self.id_builder - 1
    }
}