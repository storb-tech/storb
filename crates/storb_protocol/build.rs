use capnpc::CompilerCommand;

fn main() {
    CompilerCommand::new()
        .file("schemas/handshake_service.capnp")
        .file("schemas/info_service.capnp")
        .file("schemas/metadata_service.capnp")
        .file("schemas/piece_service.capnp")
        .run()
        .expect("Schema compilation failed");
}
