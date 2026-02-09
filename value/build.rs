fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix(".")
        .file("capnp/value.capnp")
        .run()
        .expect("schema compilation failed");
}