use anyhow;
use futures::future;
use futures::stream::StreamExt;
use storb_schemas::schemas::message_capnp;
use tarpc::context;
use tarpc::serde_transport::tcp;
use tarpc::server::{self, Channel};
use tokio_serde::formats::Json;

use capnp::{message::ReaderOptions, serialize};

use neuron::NeuronConfig;

/// Example config
#[derive(Clone)]
pub struct ValidatorConfig {
    pub neuron_config: NeuronConfig,
}

/// Our main validator type
#[derive(Clone)]
struct Validator {
    config: ValidatorConfig,
}

impl Validator {
    pub fn new(config: ValidatorConfig) -> Self {
        Validator { config }
    }
}

#[tarpc::service]
pub trait ValidatorService {
    /// We pass and return raw bytes (Cap’n Proto) for flexibility.
    async fn validate(request_bytes: Vec<u8>) -> Vec<u8>;
}

/// 2) Implement the service on Validator
impl ValidatorService for Validator {
    fn validate(
        self,
        _ctx: context::Context,
        request_bytes: Vec<u8>,
    ) -> impl futures::Future<Output = Vec<u8>> {
        // Example: read a HelloRequest, build a HelloResponse.
        let response_bytes = match process_request(request_bytes) {
            Ok(bytes) => bytes,
            Err(e) => {
                eprintln!("Validator error reading request: {e}");
                Vec::new()
            }
        };
        future::ready(response_bytes)
    }
}

/// A small helper to demonstrate reading a "HelloRequest" from Cap'n Proto
/// and replying with "HelloResponse". Adjust to your schema needs.
fn process_request(request_bytes: Vec<u8>) -> anyhow::Result<Vec<u8>> {
    // 1) Deserialize the HelloRequest
    let request_reader =
        serialize::read_message(&mut &request_bytes[..], ReaderOptions::default())?;
    let request = request_reader.get_root::<message_capnp::hello_request::Reader>()?;
    let name = request.get_name()?;
    println!("Validator received name: {:?}", name);

    // 2) Build a HelloResponse via TypedBuilder
    let mut response_builder =
        capnp::message::TypedBuilder::<message_capnp::hello_response::Owned>::new_default();
    {
        let mut root = response_builder.init_root();
        root.set_reply(&format!("Hello from Validator! Name = {:?}", name));
    }

    // 3) Serialize using the builder into bytes
    let mut response_bytes = Vec::new();
    serialize::write_message(&mut response_bytes, &response_builder.into_inner())?;

    Ok(response_bytes)
}

/// 3) The main entry point for your validator
///    We'll spin up the tarpc server on 127.0.0.1:5000 for demo purposes
pub fn run() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            println!("Hello from the Storb validator.");

            // 1) Start listening with a tarpc TCP transport
            let bind_addr = "127.0.0.1:5000";
            let listener = tcp::listen(bind_addr, Json::default)
                .await
                .expect("Failed to bind validator server");

            println!("Validator server listening on {bind_addr}");

            let server = listener
                // If we only want to ignore errors, we can filter_map with `ok()`
                .filter_map(|res| async move { res.ok() })
                // Turn each incoming stream into a BaseChannel
                .map(server::BaseChannel::with_defaults)
                .for_each_concurrent(None, |channel| async move {
                    // Must await the execute future:
                    let server = Validator::new(ValidatorConfig {
                        neuron_config: NeuronConfig {
                            netuid: 0,
                            wallet_name: "default".to_string(),
                            hotkey_name: "default".to_string(),
                        },
                    });
                    // channel.execute(server.serve()).await
                    // `execute` returns a Stream, so we must drain/consume it:
                    channel
                        .execute(server.serve())
                        // Each item in this Stream is itself a Future, so we can run them:
                        .for_each_concurrent(None, |f| f)
                        .await;
                });

            tokio::spawn(server);

            println!("Validator is now running. (Press Ctrl-C to stop.)");

            // This example just sleeps indefinitely. In a real app, you might do something else
            // or wait for user input.
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use futures::stream::StreamExt; // for filter_map, for_each_concurrent
    use tarpc::serde_transport::tcp;
    use tarpc::{client, context};
    use tokio_serde::formats::Json;

    /// Convenience function to start the server on an ephemeral port,
    /// returning the actual bound address for the client to connect to.
    async fn start_server() -> std::net::SocketAddr {
        let listener = tcp::listen("127.0.0.1:0", Json::default)
            .await
            .expect("Failed to bind ephemeral port.");

        let local_addr = listener.local_addr();

        // Spawn the server in the background:
        tokio::spawn(async move {
            listener
                .filter_map(|res| async move { res.ok() })
                .map(server::BaseChannel::with_defaults)
                .for_each_concurrent(None, |channel| async {
                    let server = Validator::new(ValidatorConfig {
                        neuron_config: NeuronConfig {
                            netuid: 0,
                            wallet_name: "default".to_string(),
                            hotkey_name: "default".to_string(),
                        },
                    });
                    // For each request, we get a future. Drive them:
                    channel
                        .execute(server.serve())
                        .for_each_concurrent(None, |f| f)
                        .await;
                })
                .await;
        });

        local_addr
    }

    #[tokio::test]
    async fn test_validator_service() -> Result<()> {
        // 1) Start the validator server on ephemeral port
        let server_addr = start_server().await;

        // 2) Create a tarpc client
        let transport = tcp::connect(server_addr, Json::default).await?;
        let client = ValidatorServiceClient::new(client::Config::default(), transport).spawn();

        // 3) Build a HelloRequest (Cap'n Proto)
        let request_bytes = {
            let mut builder =
                capnp::message::TypedBuilder::<message_capnp::hello_request::Owned>::new_default();
            {
                let mut root = builder.init_root();
                root.set_name("Alice");
            }
            let mut bytes = Vec::new();
            capnp::serialize::write_message(&mut bytes, &builder.into_inner())?;
            bytes
        };

        // 4) Call `validate` on the server
        let response_bytes = client.validate(context::current(), request_bytes).await?;

        // 5) Deserialize the HelloResponse
        let response_reader = capnp::serialize::read_message(
            &mut &response_bytes[..],
            capnp::message::ReaderOptions::default(),
        )?;
        let response = response_reader.get_root::<message_capnp::hello_response::Reader>()?;
        let reply_str = response.get_reply()?;

        // 6) Assert the server reply is what we expect
        assert!(
            reply_str.to_str().unwrap().contains("Alice"),
            "Expected 'Alice' in response, got: {:?}",
            reply_str
        );
        Ok(())
    }
}
