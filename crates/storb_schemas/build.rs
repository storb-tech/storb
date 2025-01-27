fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("src/schemas")
        .file("src/schemas/store.capnp")
        .output_path("src/schemas")
        .run()
        .expect("schema compiler command");
}
