use capnp::capability::Promise;
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::RpcSystem;

use crate::schema::handshake::handshake_service;

pub struct HandshakeServiceServer {}

impl handshake_service::Server for HandshakeServiceServer {
    fn handshake(
        &mut self,
        params: handshake_service::HandshakeParams,
        mut results: handshake_service::HandshakeResults,
    ) -> Promise<(), capnp::Error> {
        Promise::ok(())
    }
}

pub struct HandshakeServiceClient {
    rpc_system: RpcSystem<Side>,
}

impl HandshakeServiceClient {
    pub fn new(rpc_system: RpcSystem<Side>) -> Self {
        Self { rpc_system }
    }
}
