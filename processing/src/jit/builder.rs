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
        //flag_builder.set("enable_simd", "true").unwrap();

        let mut isa_builder = cranelift_native::builder().unwrap_or_else(|msg| {
            panic!("host machine is unsupported: {}", msg);
        });
        isa_builder.enable("has_avx2").unwrap();
        isa_builder.enable("has_avx").unwrap();
        isa_builder.enable("has_sse41").unwrap();
        let isa = isa_builder.finish(settings::Flags::new(flag_builder)).unwrap();

        let builder = JITBuilder::with_isa(isa, cranelift_module::default_libcall_names());
        let mut module = JITModule::new(builder);
        Self {
            builder_context: FunctionBuilderContext::new(),
            ctx: module.make_context(),
            module,
        }

    }

    fn compile_simd_expr(builder: &mut FunctionBuilder, base_ptr: Value, expr: Expr) -> Value {
        // We use I64X2 for 2-lane SIMD (128-bit)
        let simd_type = types::I64X2;

        match expr {
            Expr::Literal(val) => {

                // To load a constant into SIMD, we broadcast the scalar to all lanes
                let scalar = builder.ins().iconst(types::I64, val);
                builder.ins().splat(simd_type, scalar)
            }
            Expr::Add(lhs, rhs) => {
                let l = Self::compile_simd_expr(builder, base_ptr, *lhs);
                let r = Self::compile_simd_expr(builder, base_ptr, *rhs);

                builder.ins().iadd(l, r)
            },
            Expr::Col(VarId(id)) => {
                // Calculate offset: VarId * size_of(i64)
                // For simplicity, let's assume VarId is the byte offset here
                let offset = id as i32;
                let mut flags = MemFlags::new();
                flags.set_notrap(); // Safer for debugging alignment issues
                builder.ins().load(simd_type, flags, base_ptr, offset)
            }
        }
    }

    fn compile_simd_expr_wide(
        builder: &mut FunctionBuilder,
        base_ptr: Value,
        expr: Expr,
        dtype: ScalarType, // Pass the expected type here
        lanes: u16
    ) -> WideValue {
        // We use I64X2 for 2-lane SIMD (128-bit)
        let simd_type = get_simd_type(dtype, lanes);

        match expr {
            Expr::Literal(val) => {
                let scalar = builder.ins().iconst(types::I64, val);
                let vec = builder.ins().splat(simd_type, scalar);

                let vec = builder.ins().splat(simd_type, scalar);
                WideValue { low: vec, high: vec }
            }
            Expr::Add(lhs, rhs) => {
                let l = Self::compile_simd_expr_wide(builder, base_ptr, *lhs, dtype.clone(), lanes);
                let r = Self::compile_simd_expr_wide(builder, base_ptr, *rhs, dtype, lanes);

                match dtype {
                    ScalarType::I64 => WideValue {
                        low: builder.ins().iadd(l.low, r.low),
                        high: builder.ins().iadd(l.high, r.high),
                    },
                    ScalarType::F64 => WideValue {
                        low: builder.ins().fadd(l.low, r.low),
                        high: builder.ins().fadd(l.high, r.high),
                    },
                    _ => todo!()
                }
            },
            Expr::Col(VarId(id)) => {
                let offset_low = id as i32;
                let offset_high = offset_low + 16; // 16 bytes = 128 bits

                WideValue {
                    low: builder.ins().load(simd_type, MemFlags::trusted(), base_ptr, offset_low),
                    high: builder.ins().load(simd_type, MemFlags::trusted(), base_ptr, offset_high),
                }
            }
        }
    }

    fn compile_simd_loop(
        builder: &mut FunctionBuilder,
        base_ptr: Value,
        expr: Expr,
        dtype: ScalarType, // Pass the expected type here
        lanes: u16
    ) -> WideValue {
        // We use I64X2 for 2-lane SIMD (128-bit)
        let simd_type = get_simd_type(dtype, lanes);

        match expr {
            Expr::Literal(val) => {
                // Handle float vs int literals
                let scalar = match dtype {
                    ScalarType::F64 => builder.ins().f64const(val as f64),
                    _ => builder.ins().iconst(types::I64, val as i64),
                };
                let vec = builder.ins().splat(simd_type, scalar);
                WideValue { low: vec, high: vec }
            }
            Expr::Add(lhs, rhs) => {
                let l = Self::compile_simd_loop(builder, base_ptr, *lhs, dtype.clone(), lanes);
                let r = Self::compile_simd_loop(builder, base_ptr, *rhs, dtype, lanes);

                match dtype {
                    ScalarType::I64 => WideValue {
                        low: builder.ins().iadd(l.low, r.low),
                        high: builder.ins().iadd(l.high, r.high),
                    },
                    ScalarType::F64 => WideValue {
                        low: builder.ins().fadd(l.low, r.low),
                        high: builder.ins().fadd(l.high, r.high),
                    },
                    _ => todo!()
                }
            },
            Expr::Col(VarId(_id)) => {
                // In a loop, the base_ptr is already incremented to the correct row.
                // Low part: first 128 bits (offset 0)
                // High part: second 128 bits (offset 16 bytes)
                WideValue {
                    low: builder.ins().load(simd_type, MemFlags::trusted(), base_ptr, 0),
                    high: builder.ins().load(simd_type, MemFlags::trusted(), base_ptr, 16),
                }
            }
        }
    }

}


struct WideValue {
    low: Value,
    high: Value,
}

#[derive(Debug, Copy, Clone)]
enum ScalarType {
    I32,
    I64,
    F64,
}

// Map your type to a Cranelift SIMD type
fn get_simd_type(scalar: ScalarType, lanes: u16) -> Type {
    match (scalar, lanes) {
        (ScalarType::I64, 2) => types::I64X2,
        (ScalarType::I32, 4) => types::I32X4,
        (ScalarType::F64, 2) => types::F64X2,
        _ => todo!("Unsupported SIMD width"),
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use cranelift_jit::{JITBuilder, JITModule};
    use cranelift_module::{Linkage, Module};
    use std::mem;
    use std::time::Instant;
    use cranelift_codegen::ir::BlockArg;

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
        //let builder = JITBuilder::new(cranelift_module::default_libcall_names()).unwrap();
        //let mut module = JITModule::new(builder);
        let mut compiler = SIMDQueryCompiler::new();

        //let mut ctx = module.make_context();
        //let mut func_ctx = FunctionBuilderContext::new();

        // 2. Define Function Signature: fn(i64) -> i64
        // This represents a query taking one column value as input
        let mut sig = compiler.module.make_signature();

        // Use the HOST calling convention (SystemV for Linux/Mac, WindowsFastcall for Windows)
        //sig.call_conv = CallConv::triple_default(&HOST);

        // Ensure the pointer is the correct width for the architecture
        let ptr_type = compiler.module.target_config().pointer_type();

        // Parameter 0: The Input Pointer (Scalar i64)
        sig.params.push(AbiParam::new(types::I64));
        // Parameter 1: The Output Pointer (Scalar i64)
        sig.params.push(AbiParam::new(types::I64));
        compiler.ctx.func.signature = sig;

        // 3. Build the Function Body
        let mut builder = FunctionBuilder::new(&mut compiler.ctx.func, &mut compiler.builder_context);
        let entry_block = builder.create_block();
        builder.append_block_params_for_function_params(entry_block);
        builder.switch_to_block(entry_block);
        builder.seal_block(entry_block);

        let input_ptr = builder.block_params(entry_block)[0];
        let output_ptr = builder.block_params(entry_block)[1];

        // Define Algebra: (Column 0 + 10)
        let expr = Expr::Add(
            Box::new(Expr::Col(VarId(0))),
            Box::new(Expr::Literal(10))
        );

        // Compile the expression using your logic
        let result = SIMDQueryCompiler::compile_simd_expr(&mut builder, input_ptr, expr);

        builder.ins().store(MemFlags::trusted(), result, output_ptr, 0);
        builder.ins().return_(&[]);
        builder.finalize();

        // 4. Compile to Machine Code
        let id = compiler.module.declare_function("query_fn", Linkage::Export, &compiler.ctx.func.signature).unwrap();

        println!("{}", compiler.ctx.func.display());

        compiler.module.define_function(id, &mut compiler.ctx).unwrap();
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
        assert_eq!(output_vec[1], 11); // 1 + 10
    }

    #[test]
    fn test_jit_simd_add_wide() {
        // 1. Setup JIT Module
        let mut compiler = SIMDQueryCompiler::new();

        let mut sig = compiler.module.make_signature();

        // Ensure the pointer is the correct width for the architecture
        let ptr_type = compiler.module.target_config().pointer_type();

        sig.params.push(AbiParam::new(ptr_type));
        sig.params.push(AbiParam::new(ptr_type));
        compiler.ctx.func.signature = sig;

        // 3. Build the Function Body
        let mut builder = FunctionBuilder::new(&mut compiler.ctx.func, &mut compiler.builder_context);
        let entry_block = builder.create_block();
        builder.append_block_params_for_function_params(entry_block);
        builder.switch_to_block(entry_block);
        builder.seal_block(entry_block);

        let input_ptr = builder.block_params(entry_block)[0];
        let output_ptr = builder.block_params(entry_block)[1];

        // Define Algebra: (Column 0 + 10)
        let expr = Expr::Add(
            Box::new(Expr::Col(VarId(0))),
            Box::new(Expr::Literal(10))
        );

        // Compile the expression using your logic
        let result = SIMDQueryCompiler::compile_simd_expr_wide(&mut builder, input_ptr, expr, ScalarType::I64, 2);
        builder.ins().store(MemFlags::trusted(), result.low, output_ptr, 0);
        builder.ins().store(MemFlags::trusted(), result.high, output_ptr, 16);

        builder.ins().return_(&[]);
        builder.finalize();

        // 4. Compile to Machine Code
        let id = compiler.module.declare_function("query_fn", Linkage::Export, &compiler.ctx.func.signature).unwrap();

        println!("{}", compiler.ctx.func.display());

        compiler.module.define_function(id, &mut compiler.ctx).unwrap();
        compiler.module.finalize_definitions().unwrap();

        // 5. Execute
        // Define a type for our 128-bit SIMD vector (2 x i64)
        type JitFunc = unsafe extern "system" fn(*const i64, *mut i64);
        let code = compiler.module.get_finalized_function(id);
        let ptr: JitFunc = unsafe { mem::transmute(code) };

        // 3. Execution
        let input_vec: Vec<i64> = vec![0, 1, 2, 3]; // 4 elements = 256 bits
        let mut output_vec: Vec<i64> = vec![0; 4];

        let now = Instant::now();
        unsafe {
            // Note: ensure input_vec is 16-byte aligned (standard Vec usually is)
            ptr(input_vec.as_ptr(), output_vec.as_mut_ptr());
        }
        println!("duration: {:?}", now.elapsed());

        assert_eq!(output_vec[0], 10);
        assert_eq!(output_vec[1], 11);
        assert_eq!(output_vec[2], 12);
        assert_eq!(output_vec[3], 13);
    }

    #[test]
    fn test_jit_simd_add_loop() {
        // Define Algebra: (Column 0 + 10)
        let expr = Expr::Add(
            Box::new(Expr::Col(VarId(0))),
            Box::new(Expr::Literal(10))
        );

        // 1. Setup JIT Module
        let mut compiler = SIMDQueryCompiler::new();

        let mut sig = compiler.module.make_signature();

        // Ensure the pointer is the correct width for the architecture
        let ptr_type = compiler.module.target_config().pointer_type();

        // Param 0: Input Pointer, Param 1: Output Pointer, Param 2: Num Blocks (i64)
        sig.params.push(AbiParam::new(ptr_type));
        sig.params.push(AbiParam::new(ptr_type));
        sig.params.push(AbiParam::new(types::I64));
        compiler.ctx.func.signature = sig;

        // 3. Build the Function Body
        let mut builder = FunctionBuilder::new(&mut compiler.ctx.func, &mut compiler.builder_context);
        let entry_block = builder.create_block();
        builder.append_block_params_for_function_params(entry_block);
        builder.switch_to_block(entry_block);
        builder.seal_block(entry_block);

        let input_ptr = builder.block_params(entry_block)[0];
        let output_ptr = builder.block_params(entry_block)[1];
        let num_blocks = builder.block_params(entry_block)[2];

        let header_block = builder.create_block();
        builder.append_block_param(header_block, types::I64);

        let loop_block = builder.create_block();
        let exit_block = builder.create_block();

        // Initial index = 0
        let i0 = builder.ins().iconst(types::I64, 0);
        builder.ins().jump(header_block, &[BlockArg::Value(i0)]);

        // Start Loop
        builder.switch_to_block(header_block);
        let i = builder.block_params(header_block)[0];

        // 1. Check condition: if i >= num_blocks, jump to exit
        let cmp = builder.ins().icmp(IntCC::SignedLessThan, i, num_blocks);
        builder.ins().brif(cmp, loop_block, &[], exit_block, &[]); // Note: This logic varies by version;
        // typically: brif(cond, body_block, args, exit_block, args)

        builder.switch_to_block(loop_block);

        // --- Loop Body ---
        // Calculate byte offset: i * 32 (because 1 WideValue = 4 x i64 = 32 bytes)
        let thirty_two = builder.ins().iconst(types::I64, 32);
        let byte_offset = builder.ins().imul(i, thirty_two);

        // Adjust the base pointers for this iteration
        let current_input = builder.ins().iadd(input_ptr, byte_offset);
        let current_output = builder.ins().iadd(output_ptr, byte_offset);

        // Compile your expression logic
        let result = SIMDQueryCompiler::compile_simd_loop(&mut builder, current_input, expr, ScalarType::I64, 2);

        // Store results
        builder.ins().store(MemFlags::trusted(), result.low, current_output, 0);
        builder.ins().store(MemFlags::trusted(), result.high, current_output, 16);

        // Increment i and jump back
        let next_i = builder.ins().iadd_imm(i, 1);
        builder.ins().jump(header_block, &[BlockArg::Value(next_i)]);

        builder.seal_block(header_block);

        // Finalize Loop
        builder.seal_block(loop_block);
        builder.switch_to_block(exit_block);
        builder.ins().return_(&[]);
        builder.seal_block(exit_block);
        builder.finalize();

        // 4. Compile to Machine Code
        let id = compiler.module.declare_function("query_fn", Linkage::Export, &compiler.ctx.func.signature).unwrap();

        println!("{}", compiler.ctx.func.display());

        compiler.module.define_function(id, &mut compiler.ctx).unwrap();
        compiler.module.finalize_definitions().unwrap();

        // 5. Execute
        // Define a type for our 128-bit SIMD vector (2 x i64)
        type JitFunc = unsafe extern "system" fn(*const i64, *mut i64, i64);
        let code = compiler.module.get_finalized_function(id);
        let ptr: JitFunc = unsafe { mem::transmute(code) };

        // Execution
        let count = 10_000;
        let input_vec: Vec<i64> = (0..count).collect(); // 12 elements = 3 blocks
        let mut output_vec: Vec<i64> = vec![0; count as usize];
        let num_blocks = (input_vec.len() / 4) as i64;

        let now = Instant::now();
        unsafe {
            ptr(input_vec.as_ptr(), output_vec.as_mut_ptr(), num_blocks);
        }
        println!("duration: {:?}", now.elapsed());

        assert_eq!(output_vec[0], 10);
        assert_eq!(output_vec[500], 510);
        assert_eq!(output_vec[9999], 10_009);
    }

}