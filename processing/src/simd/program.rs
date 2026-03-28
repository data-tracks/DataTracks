use crate::expression::Expression;
use crate::instruction::Instruction;
use crate::simd::batch::RecordBatch;
use crate::simd::column::Column;
use crate::simd::compiler::Compiler;
use crate::simd::vm::VM;
use crate::Algebra;
use anyhow::anyhow;

pub struct Program {
    vm: VM,
    instructions: Vec<Instruction>,
    compiler: Compiler,
}

impl From<&Expression> for Program {
    fn from(expression: &Expression) -> Self {
        let mut compiler = Compiler::new();
        let mut instructions = vec![];

        compiler.compile_expr(&expression.clone(), &mut instructions);

        instructions.push(Instruction::Yield(1));

        Self::new(compiler, instructions)
    }
}

impl From<&Algebra> for Program {
    fn from(algebra: &Algebra) -> Self {
        let mut compiler = Compiler::new();
        let mut instructions = vec![];

        compiler.compile_algebra(algebra, &mut instructions);
        instructions.push(Instruction::Yield(compiler.current_schema.len()));

        // we go back to the iterator
        if let Some(parent_pc) = compiler.loop_stack.last() {
            instructions.insert(0, Instruction::Jump { target: *parent_pc });
        }

        Self::new(compiler, instructions)
    }
}


impl Program {
    
    fn new(compiler: Compiler, instructions: Vec<Instruction>) -> Program {
        let vm = VM {
            stack: Vec::new(),
            current_batch: None,
            constants: compiler.constants.clone(),
            pc: 0,
            size: 0,
            resources: vec![],
        };
        Self{
            vm,
            instructions,
            compiler,
        }
    }

    pub fn set_resource<S: AsRef<str>>(
        &mut self,
        name: S,
        iter: impl Iterator<Item = RecordBatch> + Send + Sync + 'static,
    ) -> anyhow::Result<()> {
        let index = self
            .compiler
            .resource_map
            .get(name.as_ref())
            .ok_or(anyhow!("No named resource in compiler"))?;
        self.vm.resources.insert(*index, Box::new(iter));
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn set_batch(&mut self, record_batch: RecordBatch) -> anyhow::Result<()> {
        self.vm.current_batch = Some(record_batch);
        Ok(())
    }

}

impl Iterator for Program {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
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
                        result_cols.insert(0, self.vm.stack.pop().unwrap());
                    }
                    return Some(RecordBatch {
                        num_of_rows: self.vm.current_batch.as_ref().unwrap().num_of_rows,
                        columns: result_cols,
                    });
                }
                Instruction::PushConst(id) => {
                    let column = Column::from((self.vm.constants[*id].clone(), self.vm.size));
                    self.vm.stack.push(column);
                }
                Instruction::NextTuple { resource_id } => {
                    let batch = self.vm.resources[0].next()?; // Pull a batch
                    self.vm.size = batch.num_of_rows;
                    self.vm.current_batch = Some(batch);
                }
                Instruction::Jump { target } => {
                    self.vm.pc = *target;
                }
                // ... other ops
                op => todo!("{:?}", op)
            }
            self.vm.pc += 1;
        }
        None
    }
}

#[cfg(test)]
mod test {
    use crate::expression::Expression;
    use crate::operator::Operator;
    use crate::simd::batch::RecordBatch;
    use crate::simd::column::Column;
    use crate::simd::program::Program;
    use crate::{Algebra, Schema};
    use std::time::Instant;
    use value::{Int, ValType};

    #[test]
    fn test_add() {
        let col_a = Column::Int(vec![Int(10.into()), Int(20.into()), Int(30.into())]);

        let batch = RecordBatch {
            columns: vec![col_a],
            num_of_rows: 3,
        };

        let expr = Expression::Call {
            operator: Operator::Add,
            expressions: vec![
                Expression::field("a"),
                Expression::constant(1.into()),
            ],
        };

        let mut program = Program::from(&expr);
        program.vm.size = batch.num_of_rows;
        program.vm.current_batch = Some(batch);

        let now = Instant::now();
        // 4. VALIDATE
        let batch = program
            .next()
            .unwrap()
            .columns
            .first()
            .unwrap()
            .clone();

        println!("{:?}", now.elapsed());

        let expected = Column::Int(vec![Int(11), Int(21), Int(31)]);

        assert_eq!(batch, expected);
        println!("SIMD Addition Result: {:?}", batch);
    }

    #[test]
    fn test_add_algebra() {
        let col_a = Column::Int(vec![Int(10.into()), Int(20.into()), Int(30.into())]);
        let col_b = Column::Int(vec![Int(1.into()), Int(2.into()), Int(3.into())]);

        let batch = RecordBatch {
            columns: vec![col_a.clone(), col_b],
            num_of_rows: 3,
        };

        let mut program = Program::from(&Algebra::project(
            Algebra::scan("test", Schema::fixed([("a".to_string(), ValType::Integer), ("b".to_string(), ValType::Integer)]),),
            [
                (
                    "name".to_string(),
                    Expression::Call {
                        operator: Operator::Add,
                        expressions: vec![
                            Expression::Field("a".to_string()),
                            Expression::Field("b".to_string()),
                        ],
                    },
                ),
                ("name1".to_string(), Expression::Field("a".to_string())),
            ],
        ));

        program
            .set_resource("test", [batch].into_iter())
            .unwrap();

        assert_eq!(
            program.next(),
            Some(
                RecordBatch{ columns: vec![
                    Column::Int(vec![Int(11.into()), Int(22.into()), Int(33.into())]),
                    col_a
                ], num_of_rows: 3 }
            )
        );
    }
}
