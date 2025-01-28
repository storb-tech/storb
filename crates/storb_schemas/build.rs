fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("src/schemas")
        .file("src/schemas/message.capnp")
        .output_path("src/schemas")
        .run()
        .expect("schema compiler command");
}
