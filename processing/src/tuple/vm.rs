use crate::ExplodeState;
use value::Value;

pub struct VM {
    pub(crate) stack: Vec<Value>,
    pub(crate) current_record: Vec<Value>,
    pub(crate) constants: Vec<Value>, // The "Pool" for literals
    pub pc: usize,                    // Program Counter
    pub(crate) explode_stack: Vec<ExplodeState>,
    pub resources: Vec<Box<dyn Iterator<Item = Value> + Send + Sync>>,
}

impl Clone for VM {
    fn clone(&self) -> Self {
        Self {
            stack: self.stack.clone(),
            current_record: self.current_record.clone(),
            constants: self.constants.clone(),
            pc: self.pc,
            explode_stack: self.explode_stack.clone(),
            resources: vec![],
        }
    }
}
