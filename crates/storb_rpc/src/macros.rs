pub mod include_proto {
    #[macro_export]
    macro_rules! include_proto {
        ($schema:expr) => {
            include!(concat!(env!("OUT_DIR"), "/", $schema, "_capnp.rs"))
        };
    }
}
