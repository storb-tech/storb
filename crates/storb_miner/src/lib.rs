use anyhow;
// use capnp::{message::ReaderOptions, serialize};
use futures::future;
use futures::stream::StreamExt;
use tarpc::context;
use tarpc::serde_transport::tcp;
use tarpc::server::{self, Channel};
use tokio_serde::formats::Json;
use tracing::info;

use neuron::NeuronConfig;

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

async fn main() {
    info!("Running the Storb miner...");

    // let mut listener = tcp::listen("127.0.0.1:9003", Json::default)
    //     .await
    //     .expect("Failed to bind validator server");

    // TODO: set frame size limit?
    // listener.config_mut().max_frame_length(usize::MAX);

    // let server = {
    //     let miner = Miner::new(MinerConfig {
    //         neuron_config: NeuronConfig {
    //             netuid: 0,
    //             wallet_name: "default".to_string(),
    //             hotkey_name: "default".to_string(),
    //         },
    //     });

    //     listener
    //         // If we only want to ignore errors, we can filter_map with `ok()`
    //         .filter_map(|res| async move { res.ok() })
    //         // Turn each incoming stream into a BaseChannel
    //         .map(server::BaseChannel::with_defaults)
    //         .for_each_concurrent(None, move |channel| {
    //             let miner = miner.clone();
    //             async move {
    //                 channel
    //                     .execute(miner.serve())
    //                     // Each item in this Stream is itself a Future, so we can run them:
    //                     // what is the error?
    //                     .for_each_concurrent(None, |f| f)
    //                     .await;
    //             }
    //         })
    // };

    // tokio::spawn(server);

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
