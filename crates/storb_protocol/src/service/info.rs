use std::sync::Arc;

use capnp::capability::Promise;
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::RpcSystem;
use multiaddr::Multiaddr;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::error;

use crate::constants::PROTOCOL_VERSION;
use crate::node::{LocalNodeInfo, NodeUid};
use crate::schema::info::info_result::Status;
use crate::schema::info::{info_service, local_node_info};

#[derive(Debug, Error)]
pub enum InfoError {
    #[error("Capnp error: {0}")]
    CapnpError(#[from] capnp::Error),

    #[error("Server returned unauthorized")]
    Unauthorized,

    #[error("Server returned internal server error")]
    InternalServerError,

    #[error("Not in schema: {0}")]
    NotInSchema(#[from] capnp::NotInSchema),
}

pub struct InfoServiceServer {
    node_info: Arc<RwLock<LocalNodeInfo>>,
}

impl InfoServiceServer {
    pub fn new(node_info: Arc<RwLock<LocalNodeInfo>>) -> Self {
        Self { node_info }
    }
}

impl info_service::Server for InfoServiceServer {
    fn info(
        &mut self,
        _: info_service::InfoParams,
        mut results: info_service::InfoResults,
    ) -> Promise<(), capnp::Error> {
        let node_info = self.node_info.clone();

        Promise::from_future(async move {
            let info = node_info.read().await;
            let mut result_builder = results.get().init_result();

            result_builder.set_protocol_version(PROTOCOL_VERSION);
            result_builder.set_status(Status::Ok);

            let mut node_info_builder = result_builder.init_node_info();

            // Set UID
            let mut uid_builder = node_info_builder.reborrow().init_uid();
            match info.uid {
                Some(uid) => {
                    uid_builder.set_some(uid.0);
                }
                None => {
                    uid_builder.set_none(());
                }
            }

            // Set QUIC address
            let mut quic_addr_builder = node_info_builder.init_quic_address();
            match &info.quic_address {
                Some(addr) => {
                    let addr_bytes = addr.to_vec();
                    let mut some_builder = quic_addr_builder.init_some(addr_bytes.len() as u32);
                    for (i, &byte) in addr_bytes.iter().enumerate() {
                        some_builder.set(i as u32, byte);
                    }
                }
                None => {
                    quic_addr_builder.set_none(());
                }
            }

            Ok(())
        })
    }
}

pub struct InfoServiceClient {
    rpc_system: RpcSystem<Side>,
}

impl InfoServiceClient {
    pub fn new(rpc_system: RpcSystem<Side>) -> Self {
        Self { rpc_system }
    }

    /// Get the information for the node that you are connected to.
    pub async fn get_info(&mut self) -> Result<LocalNodeInfo, InfoError> {
        let info_service = self
            .rpc_system
            .bootstrap::<info_service::Client>(Side::Client);

        let request = info_service.info_request();
        let response = request.send().promise.await?;
        let result = response.get()?.get_result()?;

        match result.get_status()? {
            Status::Ok => {}
            Status::InternalServerError => {
                return Err(InfoError::InternalServerError);
            }
            Status::Unauthorized => {
                return Err(InfoError::Unauthorized);
            }
        }

        let node_info = result.get_node_info()?;

        // Extract UID
        let uid = match node_info.get_uid()?.which()? {
            local_node_info::uid::Some(uid_value) => Some(NodeUid(uid_value)),
            local_node_info::uid::None(()) => None,
        };

        // Extract QUIC address
        let quic_address = match node_info.get_quic_address()?.which()? {
            local_node_info::address::Some(addr_reader) => {
                let addr_list = addr_reader?;
                let mut addr_bytes = Vec::with_capacity(addr_list.len() as usize);
                for i in 0..addr_list.len() {
                    addr_bytes.push(addr_list.get(i));
                }

                match Multiaddr::try_from(addr_bytes) {
                    Ok(addr) => Some(addr),
                    Err(e) => {
                        error!("Failed to parse QUIC multiaddr: {:?}", e);
                        None
                    }
                }
            }
            local_node_info::address::None(()) => None,
        };

        Ok(LocalNodeInfo { uid, quic_address })
    }
}
