use std::sync::Arc;

use capnp::capability::Promise;
use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::{pry, RpcSystem};
use storb_core::data::piece::{Piece, PieceResponse};
use storb_core::hashes::PieceHash;
use storb_storage::Storage;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{debug, error};

use crate::constants::PROTOCOL_VERSION;
use crate::schema::piece::piece_service;

#[derive(Debug, Error)]
pub enum PieceError {
    #[error("Capnp error: {0}")]
    CapnpError(#[from] capnp::Error),

    #[error("Failed to convert: {0}")]
    ConversionError(String),

    #[error("Missing response: {0}")]
    MissingResponse(String),
}

pub struct PieceServiceServer<S: Storage> {
    store: Arc<RwLock<S>>,
}

impl<S> PieceServiceServer<S>
where
    S: Storage + Send + Sync + 'static,
{
    /// Create a new piece service server instance.
    pub fn new(store: Arc<RwLock<S>>) -> Self {
        Self { store }
    }
}

impl<S> piece_service::Server for PieceServiceServer<S>
where
    S: Storage + Send + Sync + 'static,
{
    fn store_piece(
        &mut self,
        params: piece_service::StorePieceParams,
        mut results: piece_service::StorePieceResults,
    ) -> Promise<(), capnp::Error> {
        let param_reader = pry!(params.get());
        let piece_reader = pry!(param_reader.get_piece());

        // let protocol_version = piece_reader.get_protocol_version();
        let size = piece_reader.get_size();
        let piece_data = pry!(piece_reader.get_data());

        if piece_data.len() as u64 != size {
            let err_str = format!("Piece data size and reported size do not match: data size = {}, indicated size = {}", piece_data.len(), size);
            debug!(err_str);
            return Promise::err(capnp::Error::failed(err_str));
        }

        let hash_raw = blake3::hash(&piece_data);
        let hash_bytes = *hash_raw.as_bytes();
        let piece_hash = PieceHash::new(hash_bytes);
        let piece_data_vec = piece_data.to_vec();

        let store = self.store.clone();

        Promise::from_future(async move {
            // Store the piece
            let write_guard = store.write().await;
            if let Err(err) = write_guard.write(&piece_hash, &piece_data_vec) {
                error!("Failed to store data: {err}");
                return Err(capnp::Error::failed("Failed to store data".to_string()));
            }
            drop(write_guard);

            let mut result = results.get();
            result.set_piece_hash(&hash_bytes)?;

            Ok(())
        })
    }

    fn get_piece(
        &mut self,
        params: piece_service::GetPieceParams,
        mut results: piece_service::GetPieceResults,
    ) -> Promise<(), capnp::Error> {
        let param_reader = pry!(params.get());
        let piece_hash_reader = pry!(param_reader.get_piece_hash());

        let piece_hash = {
            let s = match piece_hash_reader.as_slice() {
                Some(s) => s,
                None => {
                    return Promise::err(capnp::Error::failed(
                        "Failed piece hash to slice conversion".to_string(),
                    ));
                }
            };
            let bytes = match s.try_into() {
                Ok(b) => b,
                Err(err) => {
                    error!("Failed to convert bytes: {err}");
                    return Promise::err(capnp::Error::failed(
                        "Failed to convert to bytes".to_string(),
                    ));
                }
            };
            PieceHash::new(bytes)
        };

        let store = self.store.clone();

        Promise::from_future(async move {
            let read_guard = store.read().await;
            let data = match read_guard.read(&piece_hash) {
                Ok(data) => data,
                Err(err) => {
                    error!("Failed to get data from storage: {err}");
                    return Err(capnp::Error::failed("Failed to get data".to_string()));
                }
            };
            drop(read_guard);

            // TODO: does this properly set things?
            let result = results.get();
            let mut piece_builder = result.init_piece();
            piece_builder.set_protocol_version(PROTOCOL_VERSION);
            piece_builder.set_size(data.len() as u64);
            piece_builder.set_data(&data);
            // piece_builder.into_reader();

            Ok(())
        })
    }
}

pub struct PieceServiceClient {
    rpc_system: RpcSystem<Side>,
}

impl PieceServiceClient {
    pub fn new(rpc_system: RpcSystem<Side>) -> Self {
        Self { rpc_system }
    }

    /// Store a piece on the server.
    pub async fn store_piece(&mut self, piece: Piece) -> Result<PieceHash, PieceError> {
        let piece_client: piece_service::Client = self.rpc_system.bootstrap(Side::Server);
        let mut store_req = piece_client.store_piece_request();

        // {
        let params = store_req.get();

        let mut piece_builder = params.init_piece();
        piece_builder.set_protocol_version(PROTOCOL_VERSION);
        let payload = piece.data;
        piece_builder.set_size(payload.len() as u64);
        piece_builder.set_data(&payload);
        // }

        let resp = store_req.send().promise.await?;
        if !resp.get()?.has_piece_hash() {
            return Err(PieceError::MissingResponse(
                "Missing piece hash response".to_string(),
            ));
        }
        let piece_hash = {
            let resp_hash = resp.get()?.get_piece_hash()?;
            let hash_slice = resp_hash.as_slice().ok_or(PieceError::ConversionError(
                "Failed to get piece hash as slice".to_string(),
            ))?;
            PieceHash::new(
                hash_slice
                    .try_into()
                    .map_err(|err| PieceError::ConversionError(format!("{err}")))?,
            )
        };

        Ok(piece_hash)
    }

    pub async fn get_piece(&mut self, piece_hash: PieceHash) -> Result<PieceResponse, PieceError> {
        let piece_client: piece_service::Client = self.rpc_system.bootstrap(Side::Server);
        let mut get_req = piece_client.get_piece_request();
        let req_piece_hash = &piece_hash.to_vec()[..];
        get_req.get().set_piece_hash(req_piece_hash)?;

        let resp = get_req.send().promise.await?;
        if !resp.get()?.has_piece() {
            return Err(PieceError::MissingResponse(
                "Missing piece response".to_string(),
            ));
        }
        let piece_data = resp.get()?.get_piece()?;

        Ok(PieceResponse {
            piece_size: piece_data.get_size(),
            data: piece_data.get_data()?.to_vec(),
        })
    }
}
