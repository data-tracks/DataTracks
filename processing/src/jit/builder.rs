use crate::jit::value::{Expr, VarId};
use cranelift::prelude::*;
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::Module;

pub struct QueryCompiler {
    builder_context: FunctionBuilderContext,
    ctx: codegen::Context,
    module: JITModule,
}

impl QueryCompiler {
    pub fn new() -> Self {
        let builder = JITBuilder::new(cranelift_module::default_libcall_names()).unwrap();
        let module = JITModule::new(builder);
        Self {
            builder_context: FunctionBuilderContext::new(),
            ctx: module.make_context(),
            module,
        }
    }

    // Example: Compiling a simple arithmetic expression from the tree
    fn compile_expr(&self, builder: &mut FunctionBuilder, expr: Expr) -> Value {
        match expr {
            Expr::Literal(val) => {
                let int = self.module.target_config().pointer_type();
                builder.ins().iconst(int, i64::from(val))
            },
            Expr::Add(lhs, rhs) => {
                let l = self.compile_expr(builder, *lhs);
                let r = self.compile_expr(builder, *rhs);
                builder.ins().iadd(l, r)
            },
            Expr::Col(VarId(id)) => {
                // Map VarId to a pre-defined Cranelift Stack Slot or Parameter
                builder.use_var(Variable::from_u32(id))
            }
        }
    }
}


pub struct  SIMDQueryCompiler {
    builder_context: FunctionBuilderContext,
    ctx: codegen::Context,
    module: JITModule,
}

impl SIMDQueryCompiler {

    pub fn new() -> Self {
        let mut flag_builder = settings::builder();
        //flag_builder.set("enable_simd", "true").unwrap();
        flag_builder.set("opt_level", "speed").unwrap();
        let isa_builder = cranelift_native::builder().unwrap_or_else(|msg| {
            panic!("host machine is unsupported: {}", msg);
        });
        let isa = isa_builder.finish(settings::Flags::new(flag_builder)).unwrap();

        let builder = JITBuilder::with_isa(isa, cranelift_module::default_libcall_names());
        let mut module = JITModule::new(builder);
        Self {
            builder_context: FunctionBuilderContext::new(),
            ctx: module.make_context(),
            module,
        }

    }

    fn compile_simd_expr(&self, builder: &mut FunctionBuilder, base_ptr: Value, expr: Expr) -> Value {
        // We use I64X2 for 2-lane SIMD (128-bit)
        let simd_type = types::I64X2;

        match expr {
            Expr::Literal(val) => {
                // To load a constant into SIMD, we broadcast the scalar to all lanes
                let scalar = builder.ins().iconst(types::I64, val);
                builder.ins().splat(simd_type, scalar)
            }
            Expr::Add(lhs, rhs) => {
                let l = self.compile_simd_expr(builder, base_ptr, *lhs);
                let r = self.compile_simd_expr(builder, base_ptr, *rhs);

                builder.ins().iadd(l, r)
            },
            Expr::Col(VarId(id)) => {
                // Calculate offset: VarId * size_of(i64)
                // For simplicity, let's assume VarId is the byte offset here
                let offset = id as i32;
                builder.ins().load(simd_type, MemFlags::trusted(), base_ptr, offset)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cranelift_jit::{JITBuilder, JITModule};
    use cranelift_module::{Module, Linkage};
    use std::mem;
    use std::time::Instant;
    use cranelift::codegen::isa::CallConv;
    use target_lexicon::HOST;

    #[test]
    fn test_jit_add_expression() {
        // 1. Setup JIT Module
        let builder = JITBuilder::new(cranelift_module::default_libcall_names()).unwrap();
        let mut module = JITModule::new(builder);
        let mut ctx = module.make_context();
        let mut func_ctx = FunctionBuilderContext::new();

        // 2. Define Function Signature: fn(i64) -> i64
        // This represents a query taking one column value as input
        let mut sig = module.make_signature();
        sig.params.push(AbiParam::new(types::I64));
        sig.returns.push(AbiParam::new(types::I64));
        ctx.func.signature = sig;

        // 3. Build the Function Body
        let mut builder = FunctionBuilder::new(&mut ctx.func, &mut func_ctx);
        let entry_block = builder.create_block();
        builder.append_block_params_for_function_params(entry_block);
        builder.switch_to_block(entry_block);
        builder.seal_block(entry_block);

        // Map our Newtype VarId(0) to the first function parameter
        let var0 = builder.declare_var(types::I64);
        let param0 = builder.block_params(entry_block)[0];
        builder.def_var(var0, param0);

        // Define Algebra: (Column 0 + 10)
        let expr = Expr::Add(
            Box::new(Expr::Col(VarId(0))),
            Box::new(Expr::Literal(10))
        );

        // Compile the expression using your logic
        let mut compiler = QueryCompiler::new();
        let result = compiler.compile_expr(&mut builder, expr);

        builder.ins().return_(&[result]);
        builder.finalize();

        // 4. Compile to Machine Code
        let id = compiler.module.declare_function("query_fn", Linkage::Export, &ctx.func.signature).unwrap();
        compiler.module.define_function(id, &mut ctx).unwrap();
        compiler.module.finalize_definitions().unwrap();

        // 5. Execute
        let code = compiler.module.get_finalized_function(id);
        let ptr = unsafe { mem::transmute::<*const u8, fn(i64) -> i64>(code) };

        assert_eq!(ptr(5), 15);  // 5 + 10 = 15
        assert_eq!(ptr(100), 110);
    }

    #[test]
    fn test_jit_simd_add() {
        // 1. Setup JIT Module
        let builder = JITBuilder::new(cranelift_module::default_libcall_names()).unwrap();
        let mut module = JITModule::new(builder);
        let mut ctx = module.make_context();
        let mut func_ctx = FunctionBuilderContext::new();

        // 2. Define Function Signature: fn(i64) -> i64
        // This represents a query taking one column value as input
        let mut sig = module.make_signature();

        // Use the HOST calling convention (SystemV for Linux/Mac, WindowsFastcall for Windows)
        sig.call_conv = CallConv::triple_default(&HOST);

        // Ensure the pointer is the correct width for the architecture
        let ptr_type = module.target_config().pointer_type();

        // Parameter 0: The Input Pointer (Scalar i64)
        sig.params.push(AbiParam::new(types::I64));
        // Parameter 1: The Output Pointer (Scalar i64)
        sig.params.push(AbiParam::new(types::I64));
        ctx.func.signature = sig;

        // 3. Build the Function Body
        let mut builder = FunctionBuilder::new(&mut ctx.func, &mut func_ctx);
        let entry_block = builder.create_block();
        builder.append_block_params_for_function_params(entry_block);
        builder.switch_to_block(entry_block);
        builder.seal_block(entry_block);

        let input_ptr = builder.block_params(entry_block)[0];
        let output_ptr = builder.block_params(entry_block)[1];

        // Map our Newtype VarId(0) to the first function parameter
        //let var0 = builder.declare_var(types::I64X2);
        //let param0 = builder.block_params(entry_block)[0];
        //builder.def_var(var0, param0);

        // Define Algebra: (Column 0 + 10)
        let expr = Expr::Add(
            Box::new(Expr::Col(VarId(0))),
            Box::new(Expr::Literal(10))
        );

        // Compile the expression using your logic
        let mut compiler = SIMDQueryCompiler::new();

        let result = compiler.compile_simd_expr(&mut builder, input_ptr, expr);

        builder.ins().store(MemFlags::trusted(), result, output_ptr, 0);
        builder.ins().return_(&[]);
        builder.finalize();

        // 4. Compile to Machine Code
        let id = compiler.module.declare_function("query_fn", Linkage::Export, &ctx.func.signature).unwrap();

        println!("{}", ctx.func.display());

        compiler.module.define_function(id, &mut ctx).unwrap();
        compiler.module.finalize_definitions().unwrap();

        // 5. Execute
        // Define a type for our 128-bit SIMD vector (2 x i64)
        type JitFunc = unsafe extern "system" fn(*const i64, *mut i64);
        let code = compiler.module.get_finalized_function(id);
        let ptr: JitFunc = unsafe { mem::transmute(code) };

        let input_vec: Vec<i64> = (0..4).collect();
        let mut output_vec: Vec<i64> = vec![0; 4];

        let now = Instant::now();
        unsafe {
            // Pass the pointer to the heap-allocated data
            ptr(input_vec.as_ptr(), output_vec.as_mut_ptr());
        }

        println!("duration: {:?}", now.elapsed());
        assert_eq!(output_vec[0], 10);  // 0 + 10
        assert_eq!(output_vec[1], 11); // 3 + 10
    }
}