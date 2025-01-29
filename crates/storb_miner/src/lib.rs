use anyhow;
use capnp::{message::ReaderOptions, serialize};
use futures::future;
use futures::stream::StreamExt;
use tarpc::context;
use tarpc::serde_transport::tcp;
use tarpc::server::{self, Channel};
use tokio_serde::formats::Json;
use tracing::info;

use neuron::NeuronConfig;
use storb_schemas::schemas::message_capnp;

/// Miner configuration
#[derive(Clone)]
pub struct MinerConfig {
    pub neuron_config: NeuronConfig,
}

/// The Storb miner
#[derive(Clone)]
struct Miner {
    config: MinerConfig,
}

impl Miner {
    pub fn new(config: MinerConfig) -> Self {
        Miner { config }
    }
}

#[tarpc::service]
pub trait MinerService {
    /// We pass and return raw bytes (Cap’n Proto) for flexibility.
    async fn store_piece(request_bytes: Vec<u8>) -> Vec<u8>;
}

impl MinerService for Miner {
    async fn store_piece(self, _ctx: context::Context, request_bytes: Vec<u8>) -> Vec<u8> {
        // Example: read a HelloRequest, build a HelloResponse.
        let response_bytes = match process_request(request_bytes) {
            Ok(bytes) => bytes,
            Err(e) => {
                eprintln!("Miner error reading request: {e}");
                Vec::new()
            }
        };
        response_bytes
    }
}

fn process_request(request_bytes: Vec<u8>) -> anyhow::Result<Vec<u8>> {
    // Deserialize the request
    let request_reader =
        serialize::read_message(&mut request_bytes.as_slice(), ReaderOptions::new())?;
    let request = request_reader.get_root::<message_capnp::miner_store_request::Reader>()?;

    let piece_data = request.get_data()?;

    // Hash the piece data
    let piece_id = blake3::hash(piece_data);

    // Build response with the hash
    let mut response_message = capnp::message::Builder::new_default();
    let mut response = response_message.init_root::<message_capnp::miner_store_response::Builder>();
    response.set_piece_id(&piece_id.to_hex());

    // Serialize the response
    let mut response_bytes = Vec::new();
    capnp::serialize::write_message(&mut response_bytes, &response_message)?;

    Ok(response_bytes)
}

async fn main() {
    info!("Running the Storb miner...");

    let mut listener = tcp::listen("127.0.0.1:9003", Json::default)
        .await
        .expect("Failed to bind validator server");

    // TODO: set frame size limit?
    // listener.config_mut().max_frame_length(usize::MAX);

    let server = {
        let miner = Miner::new(MinerConfig {
            neuron_config: NeuronConfig {
                netuid: 0,
                wallet_name: "default".to_string(),
                hotkey_name: "default".to_string(),
            },
        });

        listener
            // If we only want to ignore errors, we can filter_map with `ok()`
            .filter_map(|res| async move { res.ok() })
            // Turn each incoming stream into a BaseChannel
            .map(server::BaseChannel::with_defaults)
            .for_each_concurrent(None, move |channel| {
                let miner = miner.clone();
                async move {
                    channel
                        .execute(miner.serve())
                        // Each item in this Stream is itself a Future, so we can run them:
                        // what is the error?
                        .for_each_concurrent(None, |f| f)
                        .await;
                }
            })
    };

    tokio::spawn(server);

    println!("Miner is now running. (Press Ctrl-C to stop.)");

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}

/// Run the miner
pub fn run() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(main())
}
