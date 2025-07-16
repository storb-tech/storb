//! Helper libraries to set up and run the RPC system.

use std::net::{AddrParseError, SocketAddr};

use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::{twoparty, RpcSystem};
use quinn::ConnectionError;
use thiserror::Error;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::debug;

use crate::client::{create_client_endpoint, create_quic_client, ClientError, ClientOptions};

#[derive(Debug, Error)]
enum RpcError {
    #[error("Failed to run client: {0}")]
    ClientError(#[from] ClientError),

    #[error("Failed to parse socket address: {0}")]
    AddrParseError(#[from] AddrParseError),

    #[error("Lost the QUIC connection: {0}")]
    ConnectionError(#[from] ConnectionError),
}

/// Configuration options for the RPC client.
pub struct RPCClientOptions {
    /// Disable TLS
    pub insecure: bool,
    pub server_addr: SocketAddr 
}

/// Configuration options for the RPC server.
pub struct RPCServerOptions {
    pub addr: SocketAddr
}

// TODO: remove unwraps
pub async fn init_client_rpc(rpc_options: RpcClientOptions) -> Result<RpcSystem<Side>, RpcError> {
    let endpoint = create_client_endpoint(
        "0.0.0.0:0".parse()?,
        ClientOptions {
            insecure: rpc_options.insecure,
        },
    )?;
    let server_addr: SocketAddr = rpc_options.server_addr;
    let quic_conn = create_quic_client(&endpoint, server_addr).await?;
    debug!("QUIC connection established");

    // Open a bidirectional stream for RPC communication
    let (send_stream, recv_stream) = quic_conn.open_bi().await?;
    let reader = recv_stream.compat();
    let writer = send_stream.compat_write();

    let network = twoparty::VatNetwork::new(
        Box::new(reader),
        Box::new(writer),
        Side::Client,
        Default::default(),
    );

    let rpc_system = RpcSystem::new(Box::new(network), None);
    Ok(rpc_system)
}

// pub async fn init_server_rpc(rpc_options: RPCServerOptions) -> {
//     let port = rpc_options.addr.port();
//     let ip = rpc_options.addr.ip();
//     let socket = UdpSocket::bind(("0.0.0.0", quic_port))?;
//     let server_config = configure_server(ip).unwrap();
// }
