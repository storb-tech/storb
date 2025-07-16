pub mod client;
pub mod constants;
pub mod node;
pub mod rpc;
pub mod server;
pub mod service;
pub mod util;

pub mod handshake_service_capnp {
    include!(concat!(
        env!("OUT_DIR"),
        "/schemas/handshake_service_capnp.rs"
    ));
}

pub mod info_service_capnp {
    include!(concat!(env!("OUT_DIR"), "/schemas/info_service_capnp.rs"));
}

pub mod metadata_service_capnp {
    include!(concat!(
        env!("OUT_DIR"),
        "/schemas/metadata_service_capnp.rs"
    ));
}

pub mod piece_service_capnp {
    include!(concat!(env!("OUT_DIR"), "/schemas/piece_service_capnp.rs"));
}

pub mod schema {
    pub use crate::{
        handshake_service_capnp as handshake, info_service_capnp as info,
        metadata_service_capnp as metadata, piece_service_capnp as piece,
    };
}
