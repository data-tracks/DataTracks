use crate::simd::batch::RecordBatch;
use crate::simd::column::Column;
use value::Value;

pub struct VM {
    pub(crate) stack: Vec<Column>,
    pub(crate) current_batch: Option<RecordBatch>,
    pub(crate) constants: Vec<Value>, // The "Pool" for literals
    pub pc: usize,                    // Program Counter
    pub size: usize,
    pub resources: Vec<Box<dyn Iterator<Item = RecordBatch> + Send + Sync>>,
}

impl Clone for VM {
    fn clone(&self) -> Self {
        Self {
            stack: self.stack.clone(),
            current_batch: self.current_batch.clone(),
            constants: self.constants.clone(),
            pc: self.pc,
            size: 0,
            resources: vec![],
        }
    }
}
