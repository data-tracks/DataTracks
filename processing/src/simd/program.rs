use crate::instruction::Instruction;
use crate::simd::batch::RecordBatch;
use crate::simd::column::Column;
use crate::simd::compiler::Compiler;
use crate::simd::vm::VM;

pub struct Program {
    vm: VM,
    instructions: Vec<Instruction>,
    compiler: Compiler,
}

impl Program {
    fn execute_batch(&mut self) -> Option<RecordBatch> {
        let batch = self.vm.resources[0].next()?; // Pull a batch
        self.vm.current_batch = Some(batch);
        self.vm.pc = 0;

        while self.vm.pc < self.instructions.len() {
            let instr = &self.instructions[self.vm.pc];

            match instr {
                Instruction::LoadField(idx) => {
                    let col = self.vm.current_batch.as_ref().unwrap().columns[*idx].clone();
                    self.vm.stack.push(col);
                }
                Instruction::Add => {
                    let r = self.vm.stack.pop().unwrap();
                    let l = self.vm.stack.pop().unwrap();

                    match (l, r) {
                        (Column::Int(a), Column::Int(b)) => {
                            // This loop is what the CPU turns into SIMD instructions
                            let res = a.iter().zip(b.iter()).map(|(x, y)| *x + *y).collect();
                            self.vm.stack.push(Column::Int(res));
                        }
                        // Handle other type combos...
                        _ => panic!("Type mismatch"),
                    }
                }
                Instruction::Yield(amount) => {
                    // Instead of a single Value, we assemble the stack into a result batch
                    let mut result_cols = vec![];
                    for _ in 0..*amount {
                        result_cols.push(self.vm.stack.pop().unwrap());
                    }
                    return Some(RecordBatch {
                        num_of_rows: self.vm.current_batch.as_ref().unwrap().num_of_rows,
                        columns: result_cols,
                    });
                }
                // ... other ops
                _ => {}
            }
            self.vm.pc += 1;
        }
        None
    }
}

#[cfg(test)]
mod test {
    use crate::instruction::Instruction::{Add, LoadField, Yield};
    use crate::simd::batch::RecordBatch;
    use crate::simd::column::Column;
    use crate::simd::compiler::Compiler;
    use crate::simd::program::Program;
    use crate::simd::vm::VM;
    use value::Int;

    #[test]
    fn test_add() {
        let col_a = Column::Int(vec![Int(10.into()), Int(20.into()), Int(30.into())]);
        let col_b = Column::Int(vec![Int(1.into()), Int(2.into()), Int(3.into())]);

        let batch = RecordBatch {
            columns: vec![col_a, col_b],
            num_of_rows: 3,
        };

        let vm = VM {
            stack: Vec::new(),
            current_batch: None,
            constants: vec![],
            pc: 0,
            resources: vec![Box::new(vec![batch].into_iter())],
        };

        let mut program = Program {
            vm,
            instructions: vec![LoadField(0), LoadField(1), Add, Yield(1)],
            compiler: Compiler::new(),
        };

        // 4. VALIDATE
        let batch = program
            .execute_batch()
            .unwrap()
            .columns
            .first()
            .unwrap()
            .clone();

        let expected = Column::Int(vec![Int(11), Int(22), Int(33)]);

        assert_eq!(batch, expected);
        println!("SIMD Addition Result: {:?}", batch);
    }
}
